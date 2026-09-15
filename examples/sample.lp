# 示例 line protocol 文件（可直接用于 lp2tsfile）
# measurement,tags fields timestamp(ns)
cpu,host=host-a,region=cn usage_idle=90.5,usage_user=9.5,online=true,cores=8i 1704067200000000000
cpu,host=host-a,region=cn usage_idle=91.5,usage_user=8.5,online=true,cores=8i 1704067260000000000
cpu,host=host-b,region=us usage_idle=80.0,usage_user=20.0,online=false,cores=16i 1704067200000000000
cpu,host=host-b,region=us usage_idle=81.0,usage_user=19.0,online=false,cores=16i 1704067260000000000
disk,host=host-a,path=/data free=10000000000i,used=1000000000i 1704067200000000000
disk,host=host-a,path=/data free=9999999999i,used=1000000001i 1704067260000000000
disk,host=host-a,path=/tmp free=500000000i 1704067200000000000
weather,city=san\ francisco,state=ca temp=18.3,humidity=0.55,desc="foggy, mild" 1704067200000000000
weather,city=beijing,state=cn temp=5.4,humidity=0.3,desc="cold" 1704067200000000000
