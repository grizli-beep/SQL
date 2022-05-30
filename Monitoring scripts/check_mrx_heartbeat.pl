#!/usr/bin/perl
## dev: https://wiki.rencap.com/x/Sa_AAg
# info: https://wiki.rencap.com/x/IQDmAg
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

#use custom
use IO::Uncompress::Unzip qw(unzip $UnzipError);
use Time::Piece;

try {

#system 'clear'; # only for debug
our $start = time(); print  POSIX::strftime('%Y/%m/%d - %T',localtime)." START"."\n";

my $dbh_mrx_sa  = mDBIconnect::dbi_connect_MurexProd_sa();
my $dbh; my $sth; my $q="";

#settings:
my $refSec = 200; #time delay seconds


$q ="
select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE not in ('p1exg_in_fxBrokers_ntpro','p1exg_in_fix_bloomvcon_input_uat', 'p1exg_in_fxBrokers_fastmatch', 'p1exg_in_fix_all_ib', 'p1exg_in_fix_all_exo61', 'p1exg_in_fix_all_exo60', 'p1exg_in_fxBrokers_stdfixmarkit', 'p1exg_in_out_toms_blp_toms', 'p1exg_in_out_toms_blpprd2')
          union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_fix_all_ib' 
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('10:00:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('02:00:00 AM') AS TIME),8) as time_2))))
	      union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_fix_all_exo61' 
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('08:00:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('01:00:00 AM') AS TIME),8) as time_2))))
          union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_fix_all_exo60' 
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('08:00:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('01:00:00 AM') AS TIME),8) as time_2))))
          union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_fxBrokers_stdfixmarkit'
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('09:40:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('01:00:00 AM') AS TIME),8) as time_2))))
          union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_out_toms_blp_toms'
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('08:10:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('01:00:00 AM') AS TIME),8) as time_2))))
          union all
          select
              tcfg.XML as taskCfg, STM_CODE as code
          from
              murex.MUREXDB.MXMLEX_TASK_TABLE t,
              murex.MUREXDB.MXMLEX_TASK_CFG_TABLE tcfg
          where
              t.CODE = tcfg.STM_CODE
              and t.TYPE_CODE in ('FIXServiceListener', 'FIXSender')
              and t.STATUS_TAKEN = 'N'
              and tcfg.STATUS_TAKEN = 'N'
              and STM_CODE in (
          select STM_CODE from murex.MUREXDB.MXMLEX_TASK_CFG_TABLE where STM_CODE = 'p1exg_in_out_toms_blpprd2'
              and (((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_1) > (select convert(varchar,CAST(('08:10:00 AM') AS TIME),8) as time_2))
              or ((
          select convert(varchar,CAST(GETDATE() AS TIME),8) as time_2) between (select convert(varchar,CAST(('00:00:00 AM') AS TIME),8) as time_2) 
              and (select convert(varchar,CAST(('01:00:00 AM') AS TIME),8) as time_2))))
";        

$sth = dbi_sql_execute($dbh_mrx_sa,$q);
$query = $sth;

my $curentDate = strftime("%Y-%m-%d %H:%M:%S", localtime());
my $format = "%Y-%m-%d %H:%M:%S";



my $max_result=0;
my $err_txt='';

while (my $item = $query -> fetchrow_arrayref()) {
        $query -> syb_ct_get_data(1, \$xml, 0);
        my $zip =+ ${xml};
        my $xmlSyntax;
        unzip \$zip => \$xmlSyntax;

        $query -> syb_ct_get_data(2, \$task, 0);
        my $task = ${task};

=comment
        if ($task eq "p1exg_in_fix_tbricks_eqbroker") {
                #test task #exclude # dont count
                next;
        }
=cut

        my ($xmlValue) = $xmlSyntax =~ /(\<Property name\=\"(Service\'s\sNickName|FIX\sService\sNickname)\"\>[a-zA-Z0-9\.\_]{0,}\<\/Property\>)/g;

	# <Property name="Service's NickName">MXEVENTCAPTURE.IN.AZURE_FX2.FIX</Property>

        $xmlValue = substr($xmlValue, index($xmlValue, '>') + 1, rindex($xmlValue, '</') - index($xmlValue, '>') - 1);
	$xmlValue =~ s/\./\_/g;
	$xmlValue = lc($xmlValue);

        my $logPath = "/murex/logs/mxeventcapture/$xmlValue/service.log";
        #print $logPath."\n";
        my $log="ERR";
        #my $logDate = qx#ssh msk00-mrx01 'grep "35=0" $logPath | tail -1 | grep -o '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}''#;
        $log = `ssh msk00-mrx01 'grep "35=0" $logPath | tail -1'` or print "CRITICAL\n";
        #my $log = `ssh msk00-mrx01 tail -1 /murex/logs/mxeventcapture/mxeventcapture_in_quikequity_fix/service.log`;

        #print $log;

        #print index($log,"No such file or directory");
        #print "=".$@."=";
        # if ($sh_err) {
        #         print "CRITICAL\n";
        # }

	my ($logDate) = $log =~ /[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/g;
        my $result = Time::Piece -> strptime($curentDate, $format) - Time::Piece -> strptime($logDate, $format);

        if ($result < 0 ) {$result=0;}

#exception to the rule / this fix is not usual / separate stat
        if ($task eq "p1exg_out_deals_fix_RHUB_sender" || $task eq "p1exg_out_deals_fix_RHUB_listener") {
                mGrafana::send_to_grafana('mrx.graf.140101',$result); # number - stat delay
                if ($result > $refSec) {
                        my $txt = "\"".$task."\:".$result."\"";
                        print "RHUB delay:".$result."\t".$task.""."\n";
                        mGrafana::send_to_grafana('mrx.graf.1402',$txt); # text - task with over delay
                }
                if ($result > (3*$refSec)) { # alert in this case
                        $err_txt .=$task.";";
                }
                $result=0;
        } else {
#/exception to the rule


        if ($result>$max_result) {
                $max_result=$result;
        }

	if ($result > $refSec) {
		$err_txt .=$task.";";
                my $txt = "\"".$task."\t:".$result."\"";
                print "delay:".$result."\t".$task.""."\n";
                mGrafana::send_to_grafana('mrx.graf.1402',$txt); # text - task with over delay
	}
        }
}
mGrafana::send_to_grafana('mrx.graf.1401',$max_result); # number - stat delay
print "maximum delay: ".$max_result."(sec)\n\n";
######
if ($err_txt eq "") {
        print "OK\n";
        mGrafana::send_to_grafana('mrx.graf.1003',1); # OK/NOT OK - flag
}else{
        print "CRITICAL\n";
        mGrafana::send_to_grafana('mrx.graf.1003',-1); # OK/NOT OK - flag
}
######


#///////////////////////////////////////////////MUREX/CHECK/SCRIPT/////////////////////////////////////////////////////////#

# - common footer - #
  	require $FindBin::Bin."/chk_script_footer.pm";} catch { require $FindBin::Bin."/chk_script_error_catch.pm"; };
# / common footer - #

# - custom procedures - #

# / custom procedures - #
