#!/usr/bin/perl
#
## dev: https://wiki.rencap.com/x/Sa_AAg
# info: 
#
## zabbix_agent	: UserParameter=mrx.aggregat,/mrx/share/scripts/check_aggregation.pl

#
use mDBIconnect;        		# : https://wiki.rencap.com/x/PQD0Ag
use mGrafana;           		# : https://wiki.rencap.com/x/IQP0Ag
use Time::Local;				use FindBin;	
use POSIX;						use Try::Tiny;	
use Time::HiRes qw(time);		use mDBIconnect qw(dbi_sql_execute);
use strict;						use warnings;
#
try { our $start = time(); print  POSIX::strftime('%Y/%m/%d - %T',localtime)." START"."\n"; 
#
#///////////////GENERATED///////////////(C)RC/MUREX/CHECK/SCRIPT///////////////2020_12_28_12_14_20///////////////#
#
my $dbh_mrx     = mDBIconnect::dbi_connect_MurexProd();
my $dbh_quik    = mDBIconnect::dbi_connect_Quick(); 
my $dbh; my $sth; my $q="";

#>> BEGIN CODE : 
use Mail::Sendmail;
use Array::Diff;

my $i=0;
my $s="";
=outdated
my $murex1 = $dbh_mrx->prepare("
	declare \@datestart datetime
	declare \@dateend datetime
    set \@datestart = convert (varchar, dateadd(day,-4,getdate()), 112)
    set \@dateend = convert (varchar, getdate(), 112)

	--create table #EQDFDS_trade_values 	(TradeAmount numeric (32,16),Id numeric (32,0))
	--create table #EQDFDS_pos_values 	(PosAmount numeric (32,16),Id numeric (32,0), Table_name varchar (40),M_CONTRACT_ID numeric (32,0),M_STATUS char(1))
	--create table #EQDFDS_pos_values_sum (numeric (32,16),numeric (32,0))
	--create table #EQDFDS_aggr_values 	(AggrAmount numeric (32,16),Id numeric (32,0), Table_name varchar (40),M_CONTRACT_ID numeric (32,0),M_STATUS char(1))


	--insert into #EQDFDS_trade_values
	--select sum(M_OTHER_QUANTITY) TradeAmount, M_POS_ID Id
	--into #EQDFDS_trade_values
	--from murex.MUREXDB.CONSDEAL#EQDFDS#TRADE_DBF where convert (varchar, M_TRADE_DATE, 112)>=\@datestart 
    --and convert (varchar, M_TRADE_DATE, 112)<=\@dateend and M_IS_TAKEN='Y'
	--group by M_POS_ID
	
	--into #EQDFDS_pos_values
	--select M_OTHER_QUANTITY PosAmount, M_ID Id, 'EQDFDS#POS_DBF' as 'Table_name', M_CONTRACT_ID, M_STATUS
	
	--from murex.MUREXDB.CONSDEAL#EQDFDS#POS_DBF where convert (varchar, M_TRADE_DATE, 112)>=\@datestart  
    --and convert (varchar, M_TRADE_DATE, 112)<=\@dateend

    --select abs(sum((case when M_DIRECTION='Buy' then 1 else -1 end) * M_OTHER_QUANTITY)) PosAmount_sum, M_AGGR_ID Id
    --into #EQDFDS_pos_values_sum
	--from murex.MUREXDB.CONSDEAL#EQDFDS#POS_DBF where M_AGGR_ID is not null 
    --and convert (varchar, M_TRADE_DATE, 112)>=\@datestart  
    --and convert (varchar, M_TRADE_DATE, 112)<=\@dateend
    --group by M_AGGR_ID 
	
	--select M_OTHER_QUANTITY AggrAmount, M_ID Id, 'EQDFDS#AGGR_DBF' as 'Table_name', M_CONTRACT_ID, M_STATUS
	--into #EQDFDS_aggr_values
	--from murex.MUREXDB.CONSDEAL#EQDFDS#AGGR_DBF where convert (varchar, M_TRADE_DATE, 112)>=\@datestart  
    --and convert (varchar, M_TRADE_DATE, 112)<=\@dateend
	
	--select Table_name, M_CONTRACT_ID, M_STATUS from #EQDFDS_pos_values
	--union all
	--select Table_name, M_CONTRACT_ID, M_STATUS from #EQDFDS_aggr_values
	
") or die $DBI::errstr;

my $murex2 = $dbh_mrx->prepare("
	select M_ORIG_REF as 'CONTRACT',
	case when EXT.M_EVT_INTID='1.220' and EXT.M_ACTION <> 8 then 'C' else 'F' end as 'STATUS'
	from murex.MUREXDB.TRN_HDR_DBF TR ,MUREXDB.CONTRACT_DBF CON , murex.MUREXDB.TRN_EXT_DBF EXT 
	where CON.M_VERSION=TR.M_OPT_STSVER 
	and EXT.M_VERSION=CON.M_VERSION
	and TR.M_CONTRACT = CON.M_REFERENCE
	and   EXT.M_TRADE_REF=TR.M_NB 
	and M_ORIG_REF = ?
") or die $DBI::errstr;

$murex1->execute or die $DBI::errstr;

while(my $dat1 = $murex1->fetchrow_hashref)
{
$murex2->execute(int($dat1->{'M_CONTRACT_ID'})) or die $DBI::errstr;
while(my $dat2 = $murex2->fetchrow_hashref)
{
if ($dat1->{'M_STATUS'} ne $dat2->{'STATUS'})
{
$i++;
$s=$s."
 | TABLE: ".$dat1->{'Table_name'}." 
 | M_CONTRACT_ID: ".$dat1->{'M_CONTRACT_ID'}." 
 | WRONG M_STATUS: ".$dat1->{'M_STATUS'}."\n";
}
}
}

$murex1 = $dbh_mrx->prepare("
	select 'EQDFDS_TRADE_POS' Syst, t1.Id, t1.TradeAmount, t2.PosAmount
    from #EQDFDS_trade_values t1, #EQDFDS_pos_values t2 where t1.Id = t2.Id and t1.TradeAmount <> t2.PosAmount
    union all
    select 'EQDFDS_TRADE_POS' Syst, t2.Id, case when t1.TradeAmount is null then 0 end as 'TradeAmount', t2.PosAmount
    from #EQDFDS_pos_values t2 left join #EQDFDS_trade_values t1 on t2.Id=t1.Id where t1.TradeAmount is null
    union all
    select 'EQDFDS_TRADE_POS' Syst, t1.Id, t1.TradeAmount, case when t2.PosAmount is null then 0 end as 'PosAmount' 
    from #EQDFDS_trade_values t1 left join #EQDFDS_pos_values t2 on t1.Id=t2.Id where t2.PosAmount is null
") or die $DBI::errstr;

$murex1->execute or die $DBI::errstr;

while(my $dat3 = $murex1->fetchrow_hashref)
{
$i++;
my $id=0+$dat3->{'Id'};
my $trade_amount=0+$dat3->{'TradeAmount'};
my $pos_amount=0+$dat3->{'PosAmount'};
$s=$s."
 | SYSTEM: ".$dat3->{'Syst'}." 
 | M_ID: ".$id." 
 | TRADE_AMOUNT: ".$trade_amount." 
 | POS_AMOUNT: ".$pos_amount.";\n\n" ;
}

$murex1 = $dbh_mrx->prepare("
	select 'EQDFDS_POS_AGGR' Syst, t1.Id, t1.PosAmount_sum, t2.AggrAmount
    from #EQDFDS_pos_values_sum t1, #EQDFDS_aggr_values t2 where t1.Id = t2.Id and t1.PosAmount_sum <> t2.AggrAmount
    union all
    select 'EQDFDS_POS_AGGR' Syst, t2.Id, case when t1.PosAmount_sum is null then 0 end as 'PosAmount_sum', t2.AggrAmount
    from #EQDFDS_aggr_values t2 left join #EQDFDS_pos_values_sum t1 on t2.Id=t1.Id where t1.PosAmount_sum is null
    union all
    select 'EQDFDS_POS_AGGR' Syst, t1.Id, t1.PosAmount_sum, case when t2.AggrAmount is null then 0 end as 'AggrAmount' 
    from #EQDFDS_pos_values_sum t1 left join #EQDFDS_aggr_values t2 on t1.Id=t2.Id where t2.AggrAmount is null
");

$murex1->execute;

while(my $dat4 = $murex1->fetchrow_hashref)
{
$i++;
my $id=0+$dat4->{'Id'};
my $pos_amount_sum=0+$dat4->{'PosAmount_sum'};
my $aggr_amount=0+$dat4->{'AggrAmount'};
$s=$s."
 | SYSTEM: ".$dat4->{'Syst'}." 
 | M_ID: ".$id." 
 | POS_AMOUNT: ".$pos_amount_sum." 
 | AGGR_AMOUNT: ".$aggr_amount."\n\n" ;
}

=cut

if ($i gt 0)
{
print "\nCRITICAL\n"
}
else
{
print "\nOK - script is outdated, please remove from cron\n";
}
#$murex1->finish;
#



# - common footer - #
  	require $FindBin::Bin."/chk_script_footer.pm";} catch { require $FindBin::Bin."/chk_script_error_catch.pm"; };
# / common footer - #


### custom procedures ###

### common procedures ###