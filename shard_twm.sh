b.
# 若沒有先 stopBalancer(), 則 db.dropDatabase()會失敗，而且用 --eval會看不到失敗訊息。
mongo admin --port 40000 --eval "sh.stopBalancer()"
mongo twm_exp --port 40000 --eval "db.dropDatabase()"
mongo admin --port 40000 --eval "sh.startBalancer()"


mongo admin --port 40000 --eval "sh.enableSharding('twm_exp')"
mongo admin --port 40000 --eval "sh.shardCollection('twm_exp.ten_billion',{_id:'hashed'})"

# 每次 dropDatabase 就要重新設定  disableBalancing
echo disable
mongo admin --port 40000 --eval "sh.disableBalancing('twm_exp.ten_billion')"
mongo config --port 40000 --eval "db.getSiblingDB('config').collections.findOne({_id : 'twm_exp.ten_billion'}).noBalance; "
