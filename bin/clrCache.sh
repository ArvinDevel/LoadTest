#!/usr/bin/expect 
set usr arvin
set passwd arvin
set ipAddr 120.27.97.117
set timeout 300
set cmdPromt "]#|~]?"	

spawn ssh $usr@$ipAddr

expect {
 ".*password:"
{
send "$passwd\r"}

".*connecting(yes/no)?"{
send "yes\r";exp_continue
}

timeout{
exit
}

}

expect {
-re $cmdPromt {
send "sync\r"
}
}

expect {
-re $cmdPromt {
send "echo 3 > /proc/sys/vm/drop_caches\r"
}
}

send "exit\r"
expect eof
exit
