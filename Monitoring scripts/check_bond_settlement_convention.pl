#!/usr/bin/perl
use mDBIconnect;
use POSIX;

#  DBI DB connect 
my $dbh_mrx     = mDBIconnect::dbi_connect_MurexProd(); # msk00-syb01   / murex     / svcMurexWar
#  DBI DB connect

my $murex_query1 = $dbh_mrx->prepare("
select
    M_SE_TCS_L as 'Trading clauses', count(*) as Quantity
from
(
    select
        distinct M_SE_TCS_L, M_ALT_CONV_D
    from
    (
        select head.M_SE_D_LABEL, root.M_SE_LABEL, head.M_SE_LABEL, root.M_SE_MARKET, root.M_SE_TCS_L, sett.M_SE_DCONV, sett.M_SE_CACONV,  sett.M_SE_SET, udf.M_ALT_CONV_D, head.M_SE_MAT
        from MUREXDB.SE_ROOT_DBF root
        left join MUREXDB.SE_HEAD_DBF head on head.M_SE_LABEL = root.M_SE_LABEL
        left join MUREXDB.SE_TRDS_DBF sett on root.M_SE_TCS_L=sett.M_SE_TCS_L
        left join MUREXDB.TABLE#DATA#SECURITI_DBF udf on root.M_SE_LABEL=udf.M_SE_LABEL
        where head.M_SE_GROUP = 'Bond' and sett.M_SE_SET = 2 
    ) T
) T
group by M_SE_TCS_L
having count(*)>1
");

my $murex_query2 = $dbh_mrx->prepare("
select head.M_SE_D_LABEL, root.M_SE_LABEL as ROOT_M_SE_LABEL, head.M_SE_LABEL as HEAD_M_SE_LABEL, root.M_SE_MARKET, root.M_SE_TCS_L, sett.M_SE_DCONV, sett.M_SE_CACONV,  sett.M_SE_SET, udf.M_ALT_CONV_D, head.M_SE_MAT
from MUREXDB.SE_ROOT_DBF root
left join MUREXDB.SE_HEAD_DBF head on head.M_SE_LABEL = root.M_SE_LABEL
left join MUREXDB.SE_TRDS_DBF sett on root.M_SE_TCS_L=sett.M_SE_TCS_L
left join MUREXDB.TABLE#DATA#SECURITI_DBF udf on root.M_SE_LABEL=udf.M_SE_LABEL
where head.M_SE_GROUP = 'Bond' and sett.M_SE_SET = 2 and isNull(udf.M_ALT_CONV_D,'')=''
");

$murex_query1->execute or die $DBI::errstr; 
$murex_query2->execute or die $DBI::errstr;

my $i1 = 0;
my %problems1=();

while(my $dat1 = $murex_query1->fetchrow_hashref)
{
$i1++;
$problems1{$i} = {"TRDCLS"=>$dat1->{'Trading clauses'}, "QUANTITY"=>$dat1->{'Quantity'}};
}

my $i2 = 0;
my %problems2=();

while(my $dat2 = $murex_query2->fetchrow_hashref)
{
$i2++;
$problems2{$i2} = {"M_SE_D_LABEL"=>$dat2->{'M_SE_D_LABEL'}, "ROOT_M_SE_LABEL"=>$dat2->{'ROOT_M_SE_LABEL'}, "HEAD_M_SE_LABEL"=>$dat2->{'HEAD_M_SE_LABEL'}, "M_SE_MARKET"=>$dat2->{'M_SE_MARKET'}, "M_SE_TCS_L"=>$dat2->{'M_SE_TCS_L'}, "M_SE_DCONV"=>$dat2->{'M_SE_DCONV'}, "M_SE_CACONV"=>$dat2->{'M_SE_CACONV'}, "M_SE_SET"=>$dat2->{'M_SE_SET'}, "M_ALT_CONV_D"=>$dat2->{'M_ALT_CONV_D'}, "M_SE_MAT"=>$dat2->{'M_SE_MAT'}};
}


my $isok=1;
if ( scalar keys %problems1 > 0 )
{
$isok=0;
foreach (sort keys %problems1)
{
print $problems1{$_}->{TRDCLS}." - ".$problems1{$_}->{QUANTITY}."\n";
};
print "\nCRITICAL\n";
}

if ( scalar keys %problems2 > 0 )
{
$isok=0;
foreach (sort keys %problems2)
{
print $problems2{$_}->{M_SE_D_LABEL}." - ".$problems2{$_}->{ROOT_M_SE_LABEL}." - ".$problems2{$_}->{HEAD_M_SE_LABEL}." - ".$problems2{$_}->{M_SE_MARKET}." - ".$problems2{$_}->{M_SE_TCS_L}." - ".$problems2{$_}->{M_SE_DCONV}." - ".$problems2{$_}->{M_SE_CACONV}." - ".$problems2{$_}->{M_SE_SET}." - ".$problems2{$_}->{M_ALT_CONV_D}." - ".$problems2{$_}->{M_SE_MAT}."\n";
};
print "\nCRITICAL\n";
}

if ($isok = 1)
{
print "\nOK\n";
}

$murex_query1->finish;
$murex_query2->finish;
$dbh_mrx->disconnect;

print "\nTIMESTAMP:\n";
print strftime('%Y/%m/%d - %T',localtime)."\n";
print "/mrx/share/scripts/check_bond_settlement_convention.pl\n";

