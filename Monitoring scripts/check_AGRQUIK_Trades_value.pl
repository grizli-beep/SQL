#!/usr/bin/perl
## dev: https://wiki.rencap.com/x/Sa_AAg
# info:
#
#//////////////////////////////////////////////(C)RC/MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#

#use
#use strict;             use warnings;
use Time::Local;        use FindBin;
use Try::Tiny;          use Time::HiRes qw(time);
use mDBIconnect;        # : https://wiki.rencap.com/x/PQD0Ag
use mGrafana;           # : https://wiki.rencap.com/x/PQD0Ag
use POSIX;
use mDBIconnect qw(dbi_sql_execute);
#
try {
#	system 'clear'; # only for debug
  	our $start = time(); print  POSIX::strftime('%Y/%m/%d - %T',localtime)." START"."\n";

	my $dbh_mrx     = mDBIconnect::dbi_connect_MurexProd();
	my $dbh_quik    = mDBIconnect::dbi_connect_Quick();
	my $dbh; my $sth; my $q="";



#///////////////////////////////////////////////MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#
# - Arguments
#
# / Arguments


my $i = 0;
my $s="";

#murex:
$q = "
select M_EXCH_CODE , M_TRADE_ID,
case when M_SETTL_CURRENCY = 'GBP' then M_OTHER_QUANTITY*100 else M_OTHER_QUANTITY end as M_OTHER_QUANTITY,
 rtrim(M_PRIM_EXCHANGE) as M_EXCHANGE
from rafael.dbo.CONSDEAL#EQBROKER#TRADES_VIEW
where convert(varchar, M_TRADE_DATE, 112) = convert(varchar, getdate(), 112)
--and M_EXCH_CODE not in ('HEU3TQZIX2')
--and M_CONTRACT_ID is not NULL
and M_PRIM_EXCHANGE!='XGAT'
";

$sth=dbi_sql_execute($dbh_mrx,$q);

my %murex=();
while(my $dat = $sth->fetchrow_hashref){   $murex {$dat->{'M_EXCH_CODE'}}=$dat->{'M_OTHER_QUANTITY'};    }
$sth->finish;
#

#quick:
$q = "
select ExchangeCode,ClassCode,Value,TradeNum from dbo.Trades
where convert(varchar, TradeDate, 112) = convert(varchar, getdate(), 112)
and
ClassCode in (
    'HKEX_ALL',
    'LSE_SET_EXMP',
    'LSE_SET_SDGB',
    'LSE_SET_D',
    'CHIX',
    'LSE_IOB_D',
    'GW_LSE',
    'XETR',
    'BATS'
    )
or
(
substring(ClassCode,1,2)='NY' or
substring(ClassCode,1,2)='NA' or
substring(ClassCode,1,2)='HK'
)
";

$sth=dbi_sql_execute($dbh_quik,$q);

my %quik=();
while(my $dat = $sth->fetchrow_hashref){   $quik {$dat->{'ExchangeCode'}}=$dat->{'Value'};    }
$sth->finish;
#

#=
	my $quik_cnt	= (scalar keys %quik);
	my $murx_cnt	= (scalar keys %murex);
	my $sum         = $murx_cnt-$quik_cnt;

	#print "QUIK:".  $quik_cnt."\n";
	print "MUREX:". $murx_cnt."\n";
	#print "= ".     $sum."\n\n";
	mGrafana::send_to_grafana('mrx.graf.120104',$quik_cnt); #QUIK AGRQuikTrades_Value cnt
	mGrafana::send_to_grafana('mrx.graf.120303',$murx_cnt); #Equity Brokers trades / cnt

	$i=0;
	my $err=0; my $err_1=0; #err cnt
	my $id;
	foreach (sort keys %murex){
	 	$id=$_;
		if (!(exists $quik{$id})){ #not in murex
      		$err++;
      		print "Not in Quik :TradeNum = $id \n";
		}else{
            if ( sprintf("%.2f", 1*$quik{$id}) gt sprintf("%.2f", 1*$murex{$id}) ) { #value not eq
				$err_1++;
				print "Diffrent value(s): ";
				print sprintf ("%.2f", 1*$quik{$id})."=".sprintf("%.2f", 1*$murex{$id}); #rounded XX.XX
				print "\n";
				last;
				}
			}
		#$i++;
        last;
	}


	if ($err gt 0) {
    	#print RED, "\nCRITICAL\n",RESET;
    	print "\nCRITICAL\n";
    	print "\n$err deal(s) not in Quik \n";
	}

    if ($err_1 gt 0) {
    	print "\nCRITICAL\n";
    	print "\n M_EXCH_CODE = $id deal(s) with different value(s) \n";
	}

	if ($err+$err_1 eq 0){ print "\nOK\n";}



#///////////////////////////////////////////////MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#

# - common footer - #
  	require $FindBin::Bin."/chk_script_footer.pm";} catch { require $FindBin::Bin."/chk_script_error_catch.pm"; };
# / common footer - #

# - custom procedures - #
# / custom procedures - #
