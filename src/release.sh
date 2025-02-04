set -x
git commit -a -m 'first release'; git tag -a v$@ -m "First release" ; git push origin v$@
