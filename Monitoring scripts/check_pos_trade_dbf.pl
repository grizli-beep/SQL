#!/usr/bin/perl
#
#### dev: https://wiki.rencap.com/x/Sa_AAg
#### script info: 
#common
use strict;         use warnings;
use mDBIconnect;    use mGrafana; 
use Time::Local;    use FindBin;
use Try::Tiny;      use Time::HiRes qw(time);
use POSIX;

try {
#
#system 'clear'; # only for debug
  our $start = time(); print  POSIX::strftime('%Y/%m/%d - %T',localtime)." START"."\n"; 

#   DBI DB connect 
	my $dbh_mrx     = mDBIconnect::dbi_connect_MurexProd();               # msk00-syb01   / murex                   / svcMurexWar
# 	my $dbh_mrx_sa  = mDBIconnect::dbi_connect_MurexProd_sa();            # msk00-syb01   /                         / sa
# 	my $dbh_dtm     = mDBIconnect::dbi_connect_DatamartProd();            # msk00-syb01   / datamart                / svcMurexWar
#	my $dbh_raf     = mDBIconnect::dbi_connect_RafaelProd();              # msk00-syb01   / rafael                  / svcMurexWar
#	my $dbh_rtm     = mDBIconnect::dbi_connect_Michelangelo_QuikExport(); # MSSQL_02      / QuikExport              / svcMurexWar
#	my $dbh_recon   = mDBIconnect::dbi_connect_Michelangelo_mrxrecon();   # MSSQL_02      / mrxrecon                / svcMurexWar
#	my $dbh_fxmm 	  = mDBIconnect::dbi_connect_FXMM(); 	                  # FXMM			    / FXMMMurexIntegration	  /	murex
#	my $dbh_quik    = mDBIconnect::dbi_connect_Quick();                   # quik-db       / QExport                 / svcMurexWar
	my $dbh; my $sth; my $q="";
#   DBI DB connect



#///////////////////////////////////////////////MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#
# - Arguments
#
# / Arguments
my $i = 0;
my $s="";


$q="
--select 'FORTS#FEES_DBF' as 'SOURCE', M_ID from MUREXDB.FORTS#FEES_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
--select 'FORTS#FEES_POS_DBF' as 'SOURCE', M_ID from MUREXDB.FORTS#FEES_POS_DBF
--where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
--union all
--select 'QFORTS#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#QFORTS#TRADE_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
--select 'QFORTS#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#QFORTS#POS_DBF
--where M_STATUS <> 'F' and current_time() < '19:00:00' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
--union all
select 'HERMES#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#HERMES#TRADE_DBF
where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
union all
select 'HERMES#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#HERMES#POS_DBF
where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
union all
select 'TOMS_DERIV#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#TOMS_DERIV#TRADE_DBF
where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
union all
select 'TOMS_DERIV#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#TOMS_DERIV#POS_DBF
where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
union all
--select 'EQDFDS#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#EQDFDS#TRADE_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
select 'FIFO#EVENTS_DBF' as 'SOURCE', M_ID from rafael.dbo.CONSDEAL#FIFO#EVENTS_DBF
where M_STATUS_TAKEN = 'E' --and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))

union all
/*
select 'EQDFDS#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#EQDFDS#POS_DBF
where M_TRADE_DATE >= dateadd(day,-4,getdate())
and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
and (((((convert(varchar,M_TRADE_DATE,112) = convert(varchar,getdate(),112))
and current_time() > '23:59:59') or (convert(varchar,M_TRADE_DATE,112) < convert(varchar,getdate(),112)))
and M_STATUS <> 'C')
or ((convert(varchar,M_TRADE_DATE,112) <= convert(varchar,getdate(),112)) and M_STATUS NOT IN ('F', 'C')))
union all

select 'AGRQUIK#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#AGRQUIK#TRADE_DBF
where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
union all
select 'AGRQUIK#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#AGRQUIK#POS_DBF
where M_TRADE_DATE >= dateadd(day,-4,getdate())
and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
and (((convert(varchar,M_TRADE_DATE,112) <= convert(varchar,dateadd(day,-1,getdate()),112))
and M_STATUS <> 'C')  and M_ID not in (525493,525883)
or ((convert(varchar,M_TRADE_DATE,112) <= convert(varchar,getdate(),112)) and M_STATUS NOT IN ('F', 'C')))
union all
*/
--select 'EQDQUIK#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#EQDQUIK#TRADE_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
--select 'EQDQUIK#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#EQDQUIK#POS_DBF
--where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
--union all
/*select 'FORTS#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FORTS#TRADE_DBF
where M_IS_TAKEN = 0 and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
union all
select 'FORTS#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FORTS#POS_DBF
where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate())) and M_ISIN not in ('USDRUB_RSK', 'EURRUB_RSK')
and M_IS_EVENING = 0
union all*/
--select 'FXQUIK#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FXQUIK#TRADE_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
--select 'FXQUIK#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FXQUIK#POS_DBF
--where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
--union all
select 'FXMM#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FXMM#TRADE_DBF
where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
union all
select 'FXMM#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#FXMM#POS_DBF
where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
union all
select 'CQG#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#CQG#TRADE_DBF
where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
and convert(varchar, M_TIMESTAMP, 108 ) < '20:00:00'
union all
select 'CQG#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#CQG#POS_DBF
where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
and M_FULL_CONTRACT_NAME LIKE 'F%' and convert(varchar, M_TIMESTAMP, 108 ) < '20:00:00'
--union all
--select 'DERINT#TRADE_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#DERINT#TRADE_DBF
--where M_IS_TAKEN = 'N' and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
--union all
--select 'DERINT#POS_DBF' as 'SOURCE', M_ID from MUREXDB.CONSDEAL#DERINT#POS_DBF
--where M_STATUS <> 'F' and M_TIMESTAMP <= (select dateadd(minute,-2,getdate()))
union all
select 'FORTS_MARGIN_CORR' as 'SOURCE', M_ID from murex.dbo.FORTS_MARGIN_CORR 
where convert(varchar,M_TIMESTAMP,112) BETWEEN 
convert(varchar,dateadd(month,-1,current_date()),112) AND convert(varchar,current_date(),112)
and M_STATUS <> 'F'
union all
select 'VARMARG_CALYPSO_IMPORT' as 'SOURCE', M_ID
from mrxrecon.dbo.VARMARG_CALYPSO_IMPORT
where convert(varchar,M_TIMESTAMP,112) BETWEEN 
convert(varchar,dateadd(month,-1,current_date()),112) AND convert(varchar,current_date(),112)
and M_STATUS <> 'F'
union all
select 'BROKER_STATEMENTS' as 'SOURCE', ID from MUREXDB.BROKER_STATEMENTS
where RUN_DATE >= dateadd(day,-7,getdate())
and M_TIMESTAMP <= (select dateadd(minute,-3,getdate()))
and M_STATUS <> 'F'
";

my $murex = $dbh_mrx->prepare($q);

$murex->execute;

while(my $dat = $murex->fetchrow_hashref)

{
$i++;
$s=$s.$dat->{'SOURCE'}." - ".$dat->{'M_ID'}."\n";
}

print "\n$i problems:\n$s\n";

if ($i gt 0) {
print "\nCRITICAL\n"
}else{
print "OK\n";
}


$murex->finish;


#///////////////////////////////////////////////MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#

# - common footer - #
  	require $FindBin::Bin."/chk_script_footer.pm";} catch { require $FindBin::Bin."/chk_script_error_catch.pm"; };
# / common footer - #

# - custom procedures - #
# / custom procedures - #




