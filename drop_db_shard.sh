
# 若沒有先 stopBalancer(), 則 db.dropDatabase()會失敗，而且用 --eval會看不到失敗訊息。
mongo admin --port 40000 --eval "sh.stopBalancer()"
mongo rf1 --port 40000 --eval "db.dropDatabase()"
# mongo rf1   --port 40000 --eval "db.case1.drop()"
mongo admin --port 40000 --eval "sh.startBalancer()"


mongo admin --port 40000 --eval "sh.enableSharding('rf1')"
mongo admin --port 40000 --eval "sh.shardCollection('rf1.case1',{_id:'hashed'})"
mongo admin --port 40000 --eval "sh.shardCollection('rf1.ten_billion',{_id:'hashed'})"


# 每次 dropDatabase 就要重新設定  disableBalancing
# echo disable
# mongo admin --port 40000 --eval "sh.disableBalancing('rf1.case1')"
# mongo admin --port 40000 --eval "sh.disableBalancing('rf1.ten_billion')"
# mongo config --port 40000 --eval "db.getSiblingDB('config').collections.findOne({_id : 'rf1.case1'}).noBalance; "
# mongo config --port 40000 --eval "db.getSiblingDB('config').collections.findOne({_id : 'rf1.ten_billion'}).noBalance; "

mongo admin --port 40000 --eval "sh.stopBalancer()"
mongo admin --port 40000 --eval "sh.getBalancerState()"

