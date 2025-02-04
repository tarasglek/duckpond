set -x
# Update version in src/version.go using the supplied version argument
sed -i 's/var Version = ".*"/var Version = "'"$1"'"/' src/version.go
git commit -a -m 'first release'
git push
git tag -a v$@ -m "First release" 
git push origin v$@
goreleaser release
