#!/bin/bash

set -e -x

mkdir private _site public

EXPORTED_DATA="static/tasks.yaml static/instances.yaml"

META_REVIEW_DATA="static/meta_review.json"

# if [[ -n "$GCI_TOKEN" ]]; then
#   python manage.py fetch_gci_task_data private
#   python manage.py cleanse_gci_task_data private _site
#   rm -rf private/
# else
#   python manage.py fetch_deployed_data _site "$EXPORTED_DATA"
# fi

python manage.py migrate
# python manage.py import_contributors_data
# python manage.py import_openhub_data

# fetch deployed meta_review data
# python manage.py fetch_deployed_data _site "$META_REVIEW_DATA" True

if [[ -f "$META_REVIEW_DATA" ]]; then
  echo "File $META_REVIEW_DATA exists."
  # Load meta_review data from json
  python manage.py loaddata "$META_REVIEW_DATA"
else
   echo "File $META_REVIEW_DATA does not exist."
fi

# Run meta review system
python manage.py run_meta_review_system

# Dump meta_review data to json
python manage.py dumpdata meta_review > "$META_REVIEW_DATA"

python manage.py collectstatic --noinput
python manage.py distill-local public --force
