# remove_authority.py

Removes the authority (and its confidence) from a metadata field of the listed items.
Items are taken from a file of handles, one per line; the field defaults to
`dc.contributor.author`. Every value is replaced by itself without the authority, one
metadata PATCH per value, so the values keep their order and no other metadata field is
rewritten.

Dry run:
```
python remove_authority.py --dry-run --endpoint="https://dspace.vsb.cz/server/api/" --handles=./handles.txt
```

Update:
```
set ENVFILE=.env-vsb
python remove_authority.py --endpoint="https://dspace.vsb.cz/server/api/" --handles=./handles.txt
```

Another field:
```
python remove_authority.py --handles=./handles.txt --field=dc.contributor.editor
```

`handles.txt` - handle urls are accepted too, empty lines and `#` comments are skipped:
```
# items with a wrong ORCID authority
123456789/1234
https://hdl.handle.net/123456789/1235
https://dspace.vsb.cz/handle/123456789/1236
```

DSpace records every replaced value in `dc.description.provenance`, so a run leaves a line
per changed author there. Re-running is safe, values that have no authority are skipped.
The script exits with 1 when any handle failed, the failed ones are listed at the end of
the log.

## Before a production run

The REST API returns the values of one field sorted into a set keyed on `place`, so two
values of the same field sharing a `place` collapse into one and the script never sees the
second one - it would report success and leave that authority behind. A repository migrated
from DSpace 5 keeps whatever `place` the old database had, so check for collisions first:

```sql
SELECT mv.dspace_object_id, mv.place, count(*)
  FROM metadatavalue mv JOIN metadatafieldregistry f ON f.metadata_field_id = mv.metadata_field_id
  JOIN metadataschemaregistry s ON s.metadata_schema_id = f.metadata_schema_id
 WHERE s.short_id='dc' AND f.element='contributor' AND f.qualifier='author'
 GROUP BY 1,2 HAVING count(*) > 1;
```

Rows here have to be fixed in the database, this script cannot reach them.
