
# 設定初始值
begin=$(date +%Y-%m-%d_%H-%M-%S)
echo "now,doc,size(MB),storageSize(MB),chunk" >> ./log.$begin


# 每 10秒記錄一次，直到 kill 此 PID。
while [ true ]
do
  now=$(date +%Y-%m-%d\ %H:%M:%S)
  doc=`mongo localhost:40000/rf1 --quiet --eval "db.case1.count()"`
  size=`mongo localhost:40000/rf1 --quiet --eval "db.case1.stats(1024*1024).size"`
  storageSize=`mongo localhost:40000/rf1 --quiet --eval "db.case1.stats(1024*1024).storageSize"`
  chunk=`mongo localhost:40000/config --quiet --eval "db.chunks.count()"`

  echo "$now,$doc,$size,$storageSize,$chunk" >> ./log.$begin

  sleep 10
done

