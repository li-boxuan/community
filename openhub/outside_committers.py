import logging

from community.git import get_org_name
from openhub.models import OutsideCommitter, ContributionsToPortfolioProject


def get_outside_committers_data(json_object):
    data = json_object['response']['result'
                                   ]['outside_committers']['contributor']
    return data


def import_data(contributor):
    logger = logging.getLogger(__name__)
    name = contributor.get('name', None)

    try:
        (cr, create) = ContributionsToPortfolioProject.objects.get_or_create(
            **contributor['contributions_to_portfolio_projects'])
        if create:
            cr.save()
        contributor['contributions_to_portfolio_projects'] = cr
        contributor['org'] = get_org_name()
        (c, created) = OutsideCommitter.objects.get_or_create(
            **contributor
            )
        if created:
            c.save()
            logger.info('\nOutsideCommitter %s has been saved' % name)
    except Exception as ex:
        logger.error(
            'Something went wrong saving this OutsideCommitter %s: %s'
            % (name, ex))
