sudo -i -u postgres bash << EOF
whoami
dropdb -e marketplace
createdb -e -O marketplace marketplace
# pgloader wexac/agriItems.db postgresql://marketplace:marketplace@/marketplace
EOF
echo "Out"
whoami


