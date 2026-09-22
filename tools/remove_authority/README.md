# remove_authority.py

Removes the authority, and the confidence that belongs to it, from every value of a
metadata field that has one. Items are taken from a file of handles, one per line; the
field defaults to `dc.contributor.author`. Each value is replaced by itself without the
authority, one metadata PATCH per value, so the values keep their order and no other
metadata field is rewritten.

**Run the SQL check in [Before a production run](#before-a-production-run) first.** There is
one state of the database in which this tool overwrites an author name.

Dry run:
```
set ENVFILE=.env-vsb
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

Values whose authority starts with `virtual::` come from a relationship and are left alone,
the run logs how many it skipped. Re-running is safe, values without an authority are
skipped. Our DSpace records every replaced value in `dc.description.provenance`, so a run
leaves a line per changed author there.

The script exits with 1 when any handle failed, the failed ones are listed at the end of
the log.

## Before a production run

The REST API returns the values of one field sorted into a set keyed on `place`, so two
values of the same field sharing a `place` collapse into one in the response - while the
server still patches against its own uncollapsed list. Every index after the collision is
then off by one, and the PATCH **replaces a different author's value**: that author's name
is overwritten with the name of the one we meant to clean. A repository migrated from
DSpace 5 keeps whatever `place` the old database had, so check for collisions first:

```sql
SELECT mv.dspace_object_id, mv.place, count(*)
  FROM metadatavalue mv JOIN metadatafieldregistry f ON f.metadata_field_id = mv.metadata_field_id
  JOIN metadataschemaregistry s ON s.metadata_schema_id = f.metadata_schema_id
 WHERE s.short_id='dc' AND f.element='contributor' AND f.qualifier='author'
 GROUP BY 1,2 HAVING count(*) > 1;
```

Change `short_id`, `element` and `qualifier` to match `--field` when you use another one.
Rows here have to be fixed in the database, this script cannot see them.

The tool cannot detect the collision up front, but it does check each item after patching
it: the values must be unchanged and in the same order, and none may still carry an
authority. When that check fails it **stops the whole run** instead of continuing, so at
most one item is affected. Do not just re-run it - fix the places first.
