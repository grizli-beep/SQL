#!/usr/bin/perl
#
## dev: https://wiki.rencap.com/x/Sa_AAg
# info: https://wiki.rencap.com/x/GQP0Ag
#
## zabbix_agent	: UserParameter=mrx.bondzero,/mrx/share/scripts/check_bond_zero_prices.pl

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
#///////////////GENERATED///////////////(C)RC/MUREX/CHECK/SCRIPT///////////////2020_12_28_12_19_48///////////////#
#
my $dbh_mrx     = mDBIconnect::dbi_connect_MurexProd();
my $dbh_quik    = mDBIconnect::dbi_connect_Quick(); 
my $dbh; my $sth; my $q=""; my @dat; my $rc;

#>> BEGIN CODE :
#

my @TRUE = (
  'XS0485927163',
  'XS0211922017',   
  'XS0212423221',   
  'XS0268230991',   
  'XS0362829326',   
  'UA050891AD02',
  'XS0292499620',
  'XS0268818118',
  'XS0237792428',
  'ZAG000101916',
  'ZAG000101924',
  'RU000A0D0G29DUP',
  'RU000A0GN9A7DUP',
  'RU000A0JP2S9DUP',
  'XS0841385411',
  'UA4000078141',
  'XS0814877071',
  'RU000A0GK2X7',
  'XS1808934589',
  'ERROR'  
);

my $true = join('\',\'',@TRUE);

#my $rc;
#my $sth;

$sth = $dbh_mrx->prepare("
  SELECT T2.M_SE_CODE, T2.M_SE_I_CODE, T2.M_SE_D_LABEL FROM MUREXDB.MPX_PRIC_DBF T1, MUREXDB.SE_HEAD_DBF T2 
  WHERE (T1.M_INSTRUM=T2.M_SE_LABEL) 
    AND (T1.M__INDEX_ IN (SELECT M__INDEX_ FROM MUREXDB.MPY_PRIC_DBF WHERE (M_BID=0) AND (M_ASK=0))) 
    AND (T1.M__DATE_=(SELECT MAX(M_DATE) FROM MUREXDB.MPX_HIS_DBF WHERE M_ALIAS='FO')) 
    AND (T1.M__ALIAS_='FO') 
    AND (T2.M_SE_GROUP='Bond') 
    AND (T2.M_SE_MAT>=CONVERT(DATE,GETDATE()))
    AND T2.M_SE_CODE not in ('$true')
    AND T2.M_SE_D_LABEL not like 'AALLN TEST%'
") or die $DBI::errstr;

$sth->execute or die $DBI::errstr;

my $error_message = "";

while(@dat = $sth->fetchrow_array) {
  $error_message .= "M_SE_CODE:" . $dat[0] . " M_SE_I_CODE:" . $dat[1] . " M_SE_D_LABEL:" . $dat[2] . "\n";
}

$sth->finish;


if ($error_message ne "") {
  print "CRITICAL | error \n$error_message\n";
} else {
  print "OK: No error\n";
}

# - common footer - #
  	require $FindBin::Bin."/chk_script_footer.pm";} catch { require $FindBin::Bin."/chk_script_error_catch.pm"; };
# / common footer - #


### custom procedures ###

### common procedures ###


