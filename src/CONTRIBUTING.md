For dev use:

```bash
watchexec -e go -r -- go run $(ls *.go | grep -v '_test\.go$') -port 8881
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
curl -X POST -d "SELECT now() AS current_time" http://localhost:8081/query
```

We have a special cli flag to short-circuit this for testing:

```bash
echo 'select now()' | ./icebase -post /query
```