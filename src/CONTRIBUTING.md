For dev use:

```bash
watchexec -e go -r -- go run *.go -port 8081
```

Then you can do:

```bash
curl -X POST -d "SELECT now() AS current_time" http://localhost:8081/query
```

We have a special cli flag to short-circuit this for testing:

```bash
echo 'select now()' | ./icebase -post /query
```