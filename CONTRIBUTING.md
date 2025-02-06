For dev use:

```bash
watchexec -e go -r -- ./duckpond.sh -port 8881
```

```bash
watchexec -e go -r -- go build
```

tests:
```
go test -v
```

Then you can do:

```bash
curl -X POST -d "SELECT now() AS current_time" http://localhost:8881/query
```

Try some sample in *.sql files:
```bash
curl -X POST  --data-binary @test/query/query_uuid.sql http://localhost:8881/query
```


We have a special cli flag to short-circuit this for testing:

```bash
echo 'select now()' | ./duckpond -post /query
```


```
github-to-sops sops exec-env ../credentials/tigris.enc.json "go test -run TestStressTest"
```ls