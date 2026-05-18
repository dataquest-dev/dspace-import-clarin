# add_metadata.py

This script adds new metadata to items that are missing it. The values for this new metadata are taken from the existing input metadata field.
```
python add_metadata.py --to_mtd_field dc.date.issued --from_mtd_field dc.date.submitted dc.date.committed dc.date.defense dc.date
```
Dry run:
```
python add_metadata.py --dry-run --endpoint="http://dev-5.pc:86/server/api/" --to_mtd_field dc.date.issued --from_mtd_field dc.date.submitted dc.date.committed dc.date.defense dc.date
```

## TUL fix date format in dc.date.issued

```
set ENVFILE=.env-tul
python add_metadata.py --fix-date-format --endpoint="https://dspace.tul.cz/server/api/" --dry-run
```

## TUL update dc.date.issued

```
set ENVFILE=.env-tul
python add_metadata.py --endpoint="https://dspace.tul.cz/server/api/" --to_mtd_field dc.date.issued --from_mtd_field dc.date.submitted dc.date.committed dc.date.defense dc.date --dry-run
```

## TUL update dc.date.issued second time
```
set ENVFILE=.env-tul
python add_metadata.py --endpoint="https://dspace.tul.cz/server/api" --to_mtd_field dc.date.issued --from_mtd_field dc.date.defense dc.date.submitted dc.date.committed dc.date --only=./update.issued.date.json
```