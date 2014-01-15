BEGIN{print "merging two files...."; FS =", "; labels[0] = "[INSERT]";labels[1] = "[OVERALL]"}

#fetch values from the first file
(FNR==NR) && ($1 == "[INSERT]" || $1 == "[OVERALL]" || $1 == "[UPDATE]" || $1 == "[READ]" ) {a[$1,$2];b[$1,$2]=$3;next;}

#merge the results from the first to the second file
($1,$2) in a {
if ($2 == "RunTime(ms)")
print $1 ", " $2 ", " calc_avg(b[$1,$2],$3);
else
print $1 ", " $2 ", " calc_sum(b[$1,$2],$3);
}

function calc_sum(first, second)
{
 third = first + second;
 return third;
}

function calc_avg(first, second)
{
 third = first + second;
 third = third / 2;
 return third;
}
