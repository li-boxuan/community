import requests
from requests.exceptions import ReadTimeout
import json
import logging
from dateutil import parser

from django.utils import timezone

from community.git import get_org_name

from meta_review.models import Participant, Comment, Reaction


def parse_time(time):
    """
    parse string to datetime
    :param time: a string represents time, e.g. 2018-05-09T11:19:26Z
    :return: an offset-aware datetime object
    """
    if time is None:
        return None

    return parser.parse(time)


class MetaReviewHandler:
    """
    This is the class responsible for scraping provided information (reviews,
    reactions), processing them and dumping into Django database
    """

    def __init__(self, content, date):
        """
        Constructs a new ``MetaReviewHandler``

        :param content: Parsed JSON data
        :param date: The update date
        """
        self.logger = logging.getLogger(__name__)
        self.logger.info('this package is alive')

        self.date = date

        # save comments in memory
        self.comments = dict()
        for issue in content:
            issue = issue['issue']
            if not issue.get('pullRequest'):
                continue
            for comment in issue['pullRequest']['comments']:
                # parse time
                comment['createdAt'] = parse_time(comment['createdAt'])
                comment['lastEditedAt'] = parse_time(comment['lastEditedAt'])
                self.comments[comment['id']] = comment

        # save reactions in memory
        self.reactions = dict()
        for key, comment in self.comments.items():
            if not comment.get('reactions'):
                continue
            for reaction in comment['reactions']:
                # record receiver
                reaction['receiver'] = {
                    'login': comment['author']['login']
                }
                # record comment id
                reaction['comment_id'] = comment['id']
                # parse time
                reaction['createdAt'] = parse_time(reaction['createdAt'])
                self.reactions[reaction['id']] = reaction

        # save participants in memory
        self.participants = dict()
        for key, comment in self.comments.items():
            # get author of each comment
            author = comment['author']

            # skip if user not exist
            # this happens when account is deleted from GitHub
            if author['login']:
                self.participants[author['login']] = author

            if not comment.get('reactions'):
                continue
            for reaction in comment['reactions']:
                # get user of each reaction
                user = reaction['user']

                # skip if user not exist
                if author['login']:
                    self.participants[user['login']] = user

    def handle(self):
        """
        Scrape data, process and store in database
        """
        print('load participants to memory', flush=True)
        self.__load_participants_to_memory()
        print('load comments to memory', flush=True)
        self.__load_comments_to_memory()
        print('load reactions to memory', flush=True)
        self.__load_reactions_to_memory()

        print('dump participants to database', flush=True)
        self.__dump_participants_to_database()
        print('dump comments to database', flush=True)
        self.__dump_comments_to_database()
        print('dump reactions to database', flush=True)
        self.__dump_reactions_to_database()

        print('update score', flush=True)
        self.__update_score()
        print('update ranking', flush=True)
        self.__update_rankings()
        print('update weight factors', flush=True)
        self.__update_weight_factors()
        print('update time', flush=True)
        self.__update_time()

        print('dump participants to database', flush=True)
        self.__dump_participants_to_database()
        print('dump comments to database', flush=True)
        self.__dump_comments_to_database()
        print('dump reactions to database', flush=True)
        self.__dump_reactions_to_database()

        self.logger.info('Meta Review System finishes.')

    def __check_comment_update(self, last_edited_at, comment):
        """
        If reviewer updates their review comment after it has
        been meta-reviewed, they will be slightly punished.

        :param last_edited_at: Datetime
        :param comment: Comment object
        """
        author = comment.author
        reactions = comment.reaction_set.all()

        need_punishment = False

        # get reactions createdTime
        for reaction in reactions:
            if last_edited_at and last_edited_at > reaction.created_at:
                need_punishment = True

        if need_punishment:
            self.logger.info('%s updates review comment after it has been'
                             'meta-reviewed. 0.5 point deducted for punishment.'
                             'comment id: %s' % (author.login, comment.id))
            author.punishment += 0.5
            author.score -= 0.5

    def __load_participants_to_memory(self):
        """
        Load participants into memory
        a) create Participant objects if not exist in database
           fetch history data if exist in database and also in memory
        b) fetch history data if exist in database but not in memory yet
        """
        self.logger.info('get or create participants')
        created_cnt = 0
        existing_cnt = 0
        for key, participant in self.participants.items():
            p, created = Participant.objects.get_or_create(
                login=participant['login']
            )
            if created:
                self.logger.debug('participant %s created'
                                  % participant['login'])
                created_cnt += 1
            else:
                self.logger.debug('participant %s exists'
                                  % participant['login'])
                existing_cnt += 1

            p.name = participant['name']

            # save into memory
            self.participants[key] = p

        self.logger.info('number of newly created participant objects: %d '
                         'number of existing participant objects: %d'
                         % (created_cnt, existing_cnt))

        # load other existing object not in memory yet into memory
        # they are inactive recently, but their ranks need update
        self.logger.info('load recently inactive participants')
        load_cnt = 0
        participants_all = Participant.objects.all()
        for p in participants_all:
            if not self.participants.get(p.login):
                self.logger.debug('participant loaded = %s' % p.login)
                # load object into memory
                self.participants[p.login] = p
                load_cnt += 1
        self.logger.info('number of rest existing objects = %s' % load_cnt)

        self.logger.info('load participants into memory done,'
                         'total number = %d'
                         % (created_cnt + existing_cnt + load_cnt))

    def __load_comments_to_memory(self):
        """
        Load reviews into memory
        a) create Review objects if not exist in database
        b) fetch history data if exist in database
        """
        self.logger.info('get or create reviews')
        created_cnt = 0
        existing_cnt = 0

        # There are lots of comments and we have to use bulk_create
        # to accelerate deploy process
        old_comments = Comment.objects.all()
        old_commments_set = set()
        for old_comment in old_comments:
            old_commments_set.add(old_comment.id)

        new_comments = []
        for key, comment in self.comments.items():
            # if it is an old comment, we skip it
            if comment['id'] in old_commments_set:
                self.logger.debug('review comment %s exists'
                                  % comment['id'])
                existing_cnt += 1
            else:
                self.logger.debug('review comment %s is new'
                                  % comment['id'])
                new_comments.append(
                    Comment(id=comment['id'])
                )
                created_cnt += 1

        # use bulk create to speed up create process
        Comment.objects.bulk_create(new_comments)

        # load all comments again (old + new)
        all_comments = Comment.objects.all()

        for c in all_comments:
            comment = self.comments[c.id]
            c.body = comment['bodyText']
            c.diff = comment['diffHunk']
            c.created_at = comment['createdAt']
            c.last_edited_at = comment['lastEditedAt']
            login = comment['author']['login']
            if login:
                c.author = self.participants[login]

            # check comment update after meta-review
            self.__check_comment_update(c.last_edited_at, c)

            # save into memory
            self.comments[c.id] = c

        self.logger.info('number of newly created comment objects: %d '
                         'number of existing comment objects: %d'
                         % (created_cnt, existing_cnt))

    def __load_reactions_to_memory(self):
        """
        Load reactions into memory
        a) create Reaction objects if not exist in database
        b) fetch history data if exist in database
        """
        self.logger.info('get or create reactions')
        created_cnt = 0
        existing_cnt = 0
        for key, reaction in self.reactions.items():
            r, created = Reaction.objects.get_or_create(
                id=reaction['id']
            )
            if created:
                self.logger.debug('reaction %s created'
                                  % reaction['id'])
                created_cnt += 1
            else:
                self.logger.debug('reaction %s exists'
                                  % reaction['id'])
                existing_cnt += 1

            r.created_at = reaction['createdAt']
            r.content = reaction['content']
            giver_login = reaction['user']['login']
            if giver_login:
                r.giver = self.participants[giver_login]
            receiver_login = reaction['receiver']['login']
            if receiver_login:
                r.receiver = self.participants[receiver_login]
            comment_id = reaction['comment_id']
            r.review = self.comments[comment_id]

            # save into memory
            self.reactions[key] = r

        self.logger.info('number of newly created reaction objects: %d '
                         'number of existing reaction objects: %d'
                         % (created_cnt, existing_cnt))

    def __update_time(self):
        """
        Update last_active_at attribute of each participant

        Note this does not accurately reflect the last time they were
        active in the community.

        First, it relies on the accuracy of issues.json fetched from
        gh-board repo.

        Second, this field should instead be interpreted as 'the last
        time the participant had impact on the meta-review system'. This
        is the last time among three things: the last time they created/edited
        a comment, the last time they did a meta-review, the last time
        their review received a meta-review.
        """
        self.logger.info('start updating last active time of all participants')
        for key, participant in self.participants.items():
            old_active_time = participant.last_active_at

            # check last time they created/edited a comment
            for comment in participant.comment_set.all():
                if participant.last_active_at is None:
                    participant.last_active_at = comment.created_at
                if comment.created_at > participant.last_active_at:
                    participant.last_active_at = comment.created_at
                if (comment.last_edited_at and
                        comment.last_edited_at > participant.last_active_at):
                    participant.last_active_at = comment.last_edited_at

            # check last time they did a meta-review
            for reaction in participant.give.all():
                if participant.last_active_at is None:
                    participant.last_active_at = reaction.created_at
                if reaction.created_at > participant.last_active_at:
                    participant.last_active_at = reaction.created_at

            # check last time they received a meta-review
            for reaction in participant.receive.all():
                if participant.last_active_at is None:
                    participant.last_active_at = reaction.created_at
                if reaction.created_at > participant.last_active_at:
                    participant.last_active_at = reaction.created_at

            if participant.last_active_at != old_active_time:
                self.logger.debug('%s last active time changed from %s to %s'
                                  % (participant.login, old_active_time,
                                     participant.last_active_at))

    def __update_score(self):
        """
        Calculate and update score of each participant using
        the following formula:

        Define:

        P1 = total points (weighted) of THUMBS_UP a person gets for all
             reviews he did.
        P2 = total number of THUMBS_UP a person gives to other
             people for their reviews.
        N1 = total points (weighted) of THUMBS_DOWN a person gets for all
             reviews he did.
        N2 = total number of THUMBS_DOWN a person gives to other people for
             their reviews.

        Then final score, denote by S, is as follows:

        S =  P1 - N1 + c1 * P2 + c2 * N2

        where c1 = 0.05, c2 = 0.2. One will get at least 0.1 point for a
        positive reaction they received, so we want c1 be smaller than that.
        c2 is larger because people are reluctant to give negative reactions.
        In all, bonus points (P2 and N2) aim to encourage people to do
        meta-reviews, but we don't want them to dominate.

        Also update score of each review comment.
        """
        self.logger.info('update scores of all participants')
        # coefficients of the formula
        c1, c2 = 0.05, 0.2
        for key, participant in self.participants.items():
            # parameters to be used in the formula
            p1, p2, n1, n2 = 0, 0, 0, 0
            # number of positive/negative reactions received
            pos_cnt, neg_cnt = 0, 0

            # get reactions received
            reactions_in = participant.receive.all()
            for reaction in reactions_in:
                # skip old reactions since they were counted before
                last_active_at = participant.last_active_at
                if last_active_at and reaction.created_at < last_active_at:
                    self.logger.debug('reaction created at %s, receiver '
                                      'last active at %s, skip'
                                      % (reaction.created_at,
                                         participant.last_active_at))
                    continue

                # get weight factor of the reaction giver
                weight_factor = reaction.giver.weight_factor
                if reaction.content.find('THUMBS_UP') != -1:
                    self.logger.debug('reaction received is %s, positive'
                                      % reaction.content)
                    p1 += weight_factor
                    pos_cnt += 1
                    # also update score of review comment
                    reaction.review.pos += 1
                    reaction.review.weighted_pos += weight_factor
                    reaction.review.score += weight_factor
                elif reaction.content.find('THUMBS_DOWN') != -1:
                    self.logger.debug('reaction received is %s, negative'
                                      % reaction.content)
                    n1 += weight_factor
                    neg_cnt += 1
                    # also update score of review comment
                    reaction.review.neg += 1
                    reaction.review.weighted_neg += weight_factor
                    reaction.review.score -= weight_factor
                else:
                    self.logger.debug('reaction received is %s, ignore'
                                      % reaction.content)

            # get reactions give away
            reactions_out = participant.give.all()
            for reaction in reactions_out:
                # skip old reactions since they were counted before
                last_active_at = participant.last_active_at
                if last_active_at and reaction.created_at < last_active_at:
                    self.logger.debug('reaction created at %s, giver '
                                      'last active at %s, skip'
                                      % (reaction.created_at,
                                         participant.last_active_at))
                    continue

                if reaction.content.find('THUMBS_UP') != -1:
                    self.logger.debug('reaction give away is %s, positive'
                                      % reaction.content)
                    p2 += 1
                elif reaction.content.find('THUMBS_DOWN') != -1:
                    self.logger.debug('reaction give away is %s, negative'
                                      % reaction.content)
                    n2 += 1
                else:
                    self.logger.debug('reaction give away is %s, ignore'
                                      % reaction.content)

            # update information
            participant.pos_in += pos_cnt
            participant.weighted_pos_in += p1
            participant.pos_out += p2
            participant.neg_in += neg_cnt
            participant.weighted_neg_in += n1
            participant.neg_out += n2
            self.logger.debug('update %s info, pos_in += %d, '
                              'weighted_pos_in += %.3f, pos_out += %d, '
                              'neg_in += %d, weighted_neg_in += %.3f, '
                              'neg_out += %d'
                              % (participant.login, pos_cnt, p1, p2,
                                 neg_cnt, n1, n2))

            # update score
            s = p1 - n1 + c1 * p2 + c2 * n2
            self.logger.debug('update %s score, before: %.3f, after: %.3f'
                              % (participant.login, participant.score,
                                 participant.score + s))
            participant.score += s

    def __update_rankings(self):
        """
        Calculate and update rankings based on scores by making
        use of Django built-in sorting method
        """
        self.logger.info('update rankings of all participants')

        # save participants data into database first
        self.__dump_participants_to_database()

        # make use of built-in order_by method to sort participants
        participants_all = Participant.objects.order_by('-score', '-pos_in')
        rank = 0
        last_score = -float('inf')
        for participant in participants_all:
            if rank == 0 or last_score != participant.score:
                rank += 1
                last_score = participant.score

            # update trend = rank (last time) - rank (this time)
            if participant.rank:
                if participant.trend:
                    self.logger.debug('update %s trend, before: %d, after: %d'
                                      % (participant.login, participant.trend,
                                         participant.rank - rank))
                else:
                    # if last time was the first time they get a rank, then
                    # they don't have trend last time
                    self.logger.debug('update %s trend, before: N/A, after: %d'
                                      % (participant.login,
                                         participant.rank - rank))
                participant.trend = participant.rank - rank
            else:
                self.logger.debug('%s has no rank before, thus no trend'
                                  % participant.login)

            # update rank
            if participant.rank:
                self.logger.debug('update %s rank, before: %d, after: %d'
                                  % (participant.login, participant.rank, rank))
            else:
                self.logger.debug('update %s rank, before: N/A, after: %d'
                                  % (participant.login, rank))
            participant.rank = rank

            # save in memory
            self.participants[participant.login] = participant

    def __update_weight_factors(self):
        """
        Based on history data and the current iteration, recalculate weight
        factors (to be used in the next iteration)

        The higher score a person has, the more impacts he has, thus his
        meta-reviews are more valuable.

        For example, in a previous iteration, Alice got 2 marks, Bob got
        0.8 marks and Charlie got 10 marks. The calculation demo would
        be as follows:

        >>> c = [2, 0.8, 10]
        >>> max_score = float(max(c))
        >>> result = [i / max_score for i in c]
        >>> print(result)
        [0.2, 0.08, 1.0]
        >>> result_adjust = [i * 0.9 + 0.1 for i in result]  # adjust
        >>> result_rounded = [round(i, 3) for i in result_adjust]
        >>> print(result_rounded)
        [0.28, 0.172, 1.0]

        Anyone who gets negative marks from previous run will have weight
        factor of 0.

        To conclude, the weight factor is a float number ranging from 0 to 1.
        """
        max_score = 1.0
        # find max score
        for key, participant in self.participants.items():
            if participant.score > max_score:
                max_score = float(participant.score)

        # calculate weight factors
        for key, participant in self.participants.items():
            if participant.score < 0:
                participant.weight_factor = 0
            else:
                participant.weight_factor = participant.score / max_score
                participant.weight_factor *= 0.9
                participant.weight_factor += 0.1

    def __dump_participants_to_database(self):
        """
        Dump participants data into Django database
        """
        self.logger.info('dump participants data into database')
        for key, participant in self.participants.items():
            try:
                participant.save()
            except Exception as ex:
                self.logger.error(
                    '\n\nSomething went wrong saving this participant %s: %s'
                    % (participant.login, ex))

    def __dump_comments_to_database(self):
        """
        Dump comments data into Django database
        """
        self.logger.info('dump review comments data into database')
        for key, comment in self.comments.items():
            try:
                comment.save()
            except Exception as ex:
                self.logger.error(
                    '\n\nSomething went wrong saving this comment %s: %s'
                    % (comment.id, ex))

    def __dump_reactions_to_database(self):
        """
        Dump reactions data into Django database
        """
        self.logger.info('dump reactions data into database')
        for key, reaction in self.reactions.items():
            try:
                reaction.save()
            except Exception as ex:
                self.logger.error(
                    '\n\nSomething went wrong saving this reaction %s: %s'
                    % (reaction.id, ex))


def handle():
    # load data from gh-board repo
    org_name = get_org_name()

    # URL to grab all issues from
    issues_url = 'http://' + org_name + '.github.io/gh-board/issues.json'

    logger = logging.getLogger(__name__)

    try:
        content = requests.get(issues_url, timeout=10)
    except ReadTimeout:
        logger.warning('Get issues from ' + issues_url +
                       ' failed. Try backup url.')
        issues_url = 'https://' + org_name + '-gh-board.netlify.com/issues.json'
        content = requests.get(issues_url, timeout=10)

    try:
        parsed_json = content.json()
    except json.JSONDecodeError:
        logger.error('JSON decode error')

    handler = MetaReviewHandler(parsed_json['issues'], timezone.now())
    handler.handle()
