from django.core.management.base import BaseCommand
from meta_review.data import handle as handle_meta_review
#import meta_review.test

class Command(BaseCommand):
    help = 'Scrape, process and store data'

    def __init__(self):
        print('Command class is instantiated')
        super().__init__()

    def handle(self, *args, **options):
        from datetime import datetime
        import pytz
        print('ready to start meta review system', datetime.now(pytz.timezone('Hongkong')))
        handle_meta_review()
