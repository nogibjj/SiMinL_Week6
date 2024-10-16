```sql
SELECT t2.Major, t1.FOD1P,
                t2.women/(t2.total) as womenshare
            FROM default.majorsDB t1
            JOIN default.womenstemDB t2 ON t1.Major = t2.Major
            ORDER BY womenshare DESC
            LIMIT 10
```

```response from databricks
[]
```

