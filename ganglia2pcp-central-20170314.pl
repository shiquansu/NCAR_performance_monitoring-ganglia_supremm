#!/usr/bin/env perl
#
# Copyright (c) 2015 Martins Innus.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#

use strict;
use warnings;

use Getopt::Std;
use Date::Parse;
use Date::Format;
use RRDs;
use PCP::LogImport;
use POSIX qw( floor );
use File::Basename qw( fileparse );

use Try::Tiny;

use vars qw( %rrd_pcp_mapping );


# Hash of hashes.
# First index is time
# Next is metric (rrd filename)
my %rrd_stats;

our $deltaT;

my $zone = "";	# default to local timezone unless -Z on command line

# Conversion routines for ganglia -> pcp
# Mostly units conversion
# Params are ganglia_metric_name, value, current time


# Generic convert counter routine
# Ganglia presents already averaged values, unaverage them based on accumulated count
# The averaging doesn't seem to be done over the sampling interval , so accumulate any leftovers 
#   to use next time, and round down for the returned value.

sub convert_counter {
	my ($base_metric, $inval, $nowtime) = @_;

	# Convert based on step size. If values can come and go, this should be more accurate.
	my $step = $rrd_pcp_mapping{$base_metric}{"stepsize"};

	if( !$step ){
		die "Stepsize not defined for $rrd_pcp_mapping{$base_metric}{'name'}\n";
	}

	my $nowval = 0;
	
	if( $rrd_pcp_mapping{$base_metric}{"prev_time"} != -1 ){
		my $deltaT = $nowtime - $rrd_pcp_mapping{$base_metric}{"prev_time"};

		#if ( $deltaT != $step ){
		#	# Have not seen enough sample data to know if this occurs regularly
		#	# May occur if a metric is not recorded for a timestep
		#	warn "Step/deltaT mismatch: $step/$deltaT for $rrd_pcp_mapping{$base_metric}{'name'} at $nowtime\n"
		#}

		$nowval = ($inval * $step) + $rrd_pcp_mapping{$base_metric}{"curr_total"};

	}
	else {
		$nowval = ($inval * $step);
	}

	$rrd_pcp_mapping{$base_metric}{"curr_total"} = $nowval;
	$rrd_pcp_mapping{$base_metric}{"prev_time"} = $nowtime;

	return floor($nowval);
}

sub convert_cpu {
	my ($base_metric, $inval, $nowtime) = @_;
	
	my $step = $rrd_pcp_mapping{$base_metric}{"stepsize"};

	if( !$step ){
		die "Stepsize not defined for $rrd_pcp_mapping{$base_metric}{'name'}\n";
	}

	my $nowmillisec = 0;

	# convert from % (out of 100) of sampling rate (default 15 sec) to total milliseconds counter

        if( $rrd_pcp_mapping{$base_metric}{"prev_time"} != -1 ){
                my $deltaT = $nowtime - $rrd_pcp_mapping{$base_metric}{"prev_time"};

                #if ( $deltaT != $step ){
                #        warn "Step/deltaT mismatch: $step/$deltaT for $rrd_pcp_mapping{$base_metric}{'name'} at $nowtime\n"
                #}

		$nowmillisec = $inval * $step * 1000.0 / 100 + $rrd_pcp_mapping{$base_metric}{"curr_total"};

        }
        else {
		$nowmillisec = $inval * $step * 1000.0 / 100;
        }

        $rrd_pcp_mapping{$base_metric}{"curr_total"} = $nowmillisec;
        $rrd_pcp_mapping{$base_metric}{"prev_time"} = $nowtime;

	return floor($nowmillisec);
}

sub convert_mem_total {
	my ($base_metric, $inval, $nowtime) = @_;

	#convert from KB to MB

	return floor($inval/1024.0);
}

sub convert_mem {
        my ($base_metric, $inval, $nowtime) = @_;

	#no value changed in convertion, ganglia reports in unit of KB

        return floor($inval);
}

sub convert_int {
        my ($base_metric, $inval, $nowtime) = @_;

        #convert from float to int
        return int($inval);
}
 
sub convert_float {
        my ($base_metric, $inval, $nowtime) = @_;

        #convert  to float
        return $inval*1.0;
}

sub convert_swap {
	my ($base_metric, $inval, $nowtime) = @_;

        #convert from KB to B
        return floor($inval*1000);
}

sub convert_boottime {
        my ($base_metric, $inval, $nowtime) = @_;

	# From boottime to uptime

        return ($nowtime - $inval);
}

sub read_local_rrdfetch_files {
    my ($r_rrd_dir, $r_rrd_file, $r_starttimearg, $r_endtimearg, $r_consolidation) = @_;
    #print "r_rrd_dir, r_rrd_file, r_starttimearg, r_endtimearg, r_consolidation",$r_rrd_dir, $r_rrd_file, $r_starttimearg, $r_endtimearg, $r_consolidation;

    #locate data file
    my $fn=$r_rrd_dir."/".$r_rrd_file;
    die "$fn not exists!\n" if ! -e $fn;
    open(my $fh, "<", $fn) or die "Can't open < input.txt: $fn";

    #read data file, then close
    my @data_array;
    while(my $line = <$fh>) {
        push(@data_array, $line);
    }
    close($fh);

    my $array_length = @data_array; 
    #print "\n length of data_array=",$array_length,"\n";
    #print "first line in data_array to split with : =",$data_array[0],"\n";

    my @start_line;
    try {
        @start_line=split /:/, $data_array[0];
        #print "startline =",@start_line,"\n";
        #print "startline[0] =",$start_line[0],"\n";
    } catch {
        warn "caught error: $_";
        print "\$data_array[0]",$data_array[0],"\n";
        print "\$fn=",$fn,"\n";
    };

    my $r_start=$start_line[0];
    #my $r_step=300;  #yellowstone data has 300 seconds step
    #my $r_step=1200;  #cheyenne data has 1200 seconds step
    # the step is reset by the $step_input in the main program
    my $r_names=$r_rrd_file;
    my $r_data=\@data_array;

    #print "r_start,r_step,r_names,r_data_refernece, r_data_content: ",$r_start," ", $r_step," ",$r_names," ",$r_data," ",@$r_data,"\n";

    #return ($r_start,$r_step,$r_names,$r_data);
    return ($r_start,$r_names,$r_data);
}

my $input_method = "read_local";

# Mapping of ganglia (rrd) metrics to pcp metrics.
#
# Undefined metrics found will be skipped
#
# Ganglia metrics are already rate converted.
#
# Need to guess at the metric metadata since there is no guarantee we are processing the logs
# on the host from which they were collected.  These values are from x86_64 linux.
#
# Could come up with different mappings based on source host if necessary.

our %rrd_pcp_mapping = (
		"boottime.rrd" => { "name"  => "kernel.all.uptime",
				    "pmid"  => pmiID(60,26,0),
				    "type"  => PM_TYPE_U32,
				    "indom" => PM_INDOM_NULL,
				    "sem"   => PM_SEM_INSTANT,
				    "units" => pmiUnits(0,1,0,0,PM_TIME_SEC,0),
				    "conv_fcn" => \&convert_boottime,
		},
		# Will have a dummy "total" instance
		# In ganglia, network metrics are all non-loopback interfaces combined
		"bytes_in.rrd" => { "name"  => "network.interface.in.bytes",
				    "pmid"  => pmiID(60,3,0),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => pmiInDom(60,3),
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_BYTE,0,0),
				    "inst"  => 1000,
				    "iname" => "total",
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_counter,
                },
		# Will have a dummy "total" instance
		"bytes_out.rrd" => { "name"  => "network.interface.out.bytes",
				    "pmid"  => pmiID(60,3,8),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => pmiInDom(60,3),
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_BYTE,0,0),
				    "inst"  => 1000,
				    "iname" => "total",
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_counter,
                },
		# These will be converted to millisec from %
		"cpu_idle.rrd" => { "name"  => "kernel.all.cpu.idle",
				    "pmid"  => pmiID(60,0,23),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,1,0,0,PM_TIME_MSEC,0),
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_cpu,
                },
		"cpu_nice.rrd" => { "name"  => "kernel.all.cpu.nice",
				    "pmid"  => pmiID(60,0,21),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,1,0,0,PM_TIME_MSEC,0),
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_cpu,
                },
		"cpu_system.rrd" => { "name"  => "kernel.all.cpu.sys",
				    "pmid"  => pmiID(60,0,22),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,1,0,0,PM_TIME_MSEC,0),
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_cpu,
                },
                "cpu_user.rrd" => { "name"  => "kernel.all.cpu.user",
				    "pmid"  => pmiID(60,0,20),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,1,0,0,PM_TIME_MSEC,0),
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_cpu,
                },
                "cpu_wio.rrd" => { "name"  => "kernel.all.cpu.wait.total",
				    "pmid"  => pmiID(60,0,35),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,1,0,0,PM_TIME_MSEC,0),
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_cpu,
                },
                "cpu_num.rrd" => { "name"  => "hinv.ncpu",
				    "pmid"  => pmiID(60,0,32),
                                    "type"  => PM_TYPE_U32,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_DISCRETE,
                                    "units" => pmiUnits(0,0,0,0,0,0),
                                    "conv_fcn" => \&convert_int,
                },
		## Need to deal with indoms here in a different way
		## Even on multi cpu machines, ganglia reports only one value
                #"cpu_speed.rrd" => { "name"  => "hinv.cpu.clock",
		#		    "pmid"  => pmiID(60,18,0),
                #                    "type"  => PM_TYPE_FLOAT,
                #                    "indom" => pmiInDom(60,0),
                #                    "sem"   => PM_SEM_DISCRETE,
                #                    "units" => pmiUnits(0,0,0,0,0,0),
		#		    "inst_pattern" => "%d",
		#		    "iname_pattern" => "cpu%d",
		#		    "inst_src" => "cpu_num.rrd",
		#		    "inst_constructed" => -1,
                #                    "conv_fcn" => \&convert_int,
                #},
		# Instance parameter denotes that the mapping is to an instance
		# ie, 3 rrd files get mapped to 3 instances of 1 pcp metric
                "load_fifteen.rrd" => { "name"  => "kernel.all.load",
				    "pmid"  => pmiID(60,2,0),
                                    "type"  => PM_TYPE_FLOAT,
                                    "indom" => pmiInDom(60,2),
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(0,0,0,0,0,0),
				    "inst"  => 15,
				    "iname" => "15 minute",
                                    "conv_fcn" => \&convert_float,
                },
                "load_five.rrd" => { "name"  => "kernel.all.load",
				    "pmid"  => pmiID(60,2,0),
                                    "type"  => PM_TYPE_FLOAT,
                                    "indom" => pmiInDom(60,2),
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(0,0,0,0,0,0),
				    "inst"  => 5,
				    "iname" => "5 minute",
                                    "conv_fcn" => \&convert_float,
                },
                "load_one.rrd" => { "name"  => "kernel.all.load",
				    "pmid"  => pmiID(60,2,0),
                                    "type"  => PM_TYPE_FLOAT,
                                    "indom" => pmiInDom(60,2),
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(0,0,0,0,0,0),
				    "inst"  => 1,
				    "iname" => "1 minute",
                                    "conv_fcn" => \&convert_float,
                },
		# Already in KB
		"mem_buffers.rrd" => { "name"  => "mem.util.bufmem",
				    "pmid"  => pmiID(60,1,4),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_KBYTE,0,0),
				    "conv_fcn" => \&convert_mem,
                },
		# Already in KB
                "mem_cached.rrd" => { "name"  => "mem.util.cached",
				    "pmid"  => pmiID(60,1,5),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_KBYTE,0,0),,
				    "conv_fcn" => \&convert_mem,
                },
                # Already in KB
                "mem_shared.rrd" => { "name"  => "mem.util.sharedmem",
                                    "pmid"  => pmiID(60,1,6),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_KBYTE,0,0),,
                                    "conv_fcn" => \&convert_mem,
                },
		# Already in KB
                "mem_free.rrd" => { "name"  => "mem.util.free",
				    "pmid"  => pmiID(60,1,2),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_KBYTE,0,0),
				    "conv_fcn" => \&convert_mem,
                },
		# Converted from KB to MB
                "mem_total.rrd" => { "name"  => "hinv.physmem",
				    "pmid"  => pmiID(60,1,9),
                                    "type"  => PM_TYPE_U32,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_DISCRETE,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_MBYTE,0,0),
				    "conv_fcn" => \&convert_mem_total,
                },
		# Dummy total instance
                "pkts_in.rrd" => { "name"  => "network.interface.in.packets",
				    "pmid"  => pmiID(60,3,1),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => pmiInDom(60,3),
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,0,1,0,0,PM_COUNT_ONE),
				    "inst"  => 1000,
				    "iname" => "total",
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_counter,
                },
		"pkts_out.rrd" => { "name"  => "network.interface.out.packets",
				    "pmid"  => pmiID(60,3,9),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => pmiInDom(60,3),
                                    "sem"   => PM_SEM_COUNTER,
                                    "units" => pmiUnits(0,0,1,0,0,PM_COUNT_ONE),
				    "inst"  => 1000,
				    "iname" => "total",
				    "curr_total" => 0,
				    "prev_time" => -1,
				    "conv_fcn" => \&convert_counter,
                },
# Is
# "run" = proc.runq.runnable
# and
# "total" = proc.nprocs
# Need to Confirm
#
#                "proc_run.rrd" => { "name"  => "",
#				    "pmid"  => pmiID(),
#                                    "type"  => ,
#                                    "indom" => ,
#                                    "sem"   => ,
#                                    "units" => ,
#                },
#                "proc_total.rrd" => { "name"  => "",
#				    "pmid"  => pmiID(),
#                                    "type"  => ,
#                                    "indom" => ,
#                                    "sem"   => ,
#                                    "units" => ,
#                },
                "swap_free.rrd" => { "name"  => "swap.free",
				    "pmid"  => pmiID(60,1,8),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_BYTE,0,0),
				    "conv_fcn" => \&convert_swap,
                },
                "swap_total.rrd" => { "name"  => "swap.length",
				    "pmid"  => pmiID(60,1,7),
                                    "type"  => PM_TYPE_U64,
                                    "indom" => PM_INDOM_NULL,
                                    "sem"   => PM_SEM_INSTANT,
                                    "units" => pmiUnits(1,0,0,PM_SPACE_BYTE,0,0),
				    "conv_fcn" => \&convert_swap,
                }
);

my $adminnode = "";
my $host = "";

# Store a list of all the metrics we find
my %rrd_found;

my %args;

my $sts = getopt('f:s:e:Z:a:h:d:t:n:', \%args);

if (!defined($sts) || $#ARGV != 0) {
    print "Usage: ganglia2pcp [-s start] [-e end] [-f output file name] [-d output file dir ] [-Z timezone] [-a adminnodename] [-h output hostname] [-t timestep] [-n input hostname] input_dir \n";
    exit(1);
}

# not implement the -t option yet, wait until the cheyenne job viewer problem solved.
# not implement the -n option yet,

# Default is the last 24 hours
# rrdtool takes a variety of formats, but we convert to unix time for simplicity.

my $localtime = time();

my $startsec = $localtime - 24*60*60;

my $starttimearg = "-s $startsec";
my $endtimearg = "-e $localtime";

if (exists($args{s}) ){
	my $starttime = $args{s};
	$startsec = str2time( $starttime ) or die "Malformed starttime: $starttime";
	$starttimearg = "-s " . $startsec;
}

if (exists($args{e})){
	my $endtime = $args{e};
	$endtimearg = str2time( $endtime ) or die "Malformed endtime: $endtime";
	$endtimearg = "-e " . $endtimearg;
}

if (exists($args{a})) {
    $adminnode = $args{a};
}

my $label_zone = "";

if (exists($args{Z})) {
    $zone = $args{Z};
    $label_zone = $zone;
    
    # Stolen from iostat2pcp, is this the right thing to do here as well ?
    #
    # PCP expects a $TZ style timezone in the archive label, so
    # we have to make up a PCP-xx:xx timezone ... note this
    # involves a sign reversal!
    
    if ($zone =~ /^[-+][0-9][0-9][0-9][0-9]/) {
        $label_zone =~ s/^\+/PCP-/;
        $label_zone =~ s/^-/PCP+/;
        $label_zone =~ s/(..)$/:$1/;
    }
    elsif ($zone ne "UTC") {
        print "rrd2pcp: Warning: unexpected timezone ($zone), reverting to UTC\n";
        $zone = "UTC";
        $label_zone = "UTC";
    }
}

# Set filename to be the standard pmlogger convention, eg 20150210.00.10

my $outfile = time2str("%Y%m%d.%k.%M", $startsec, $zone);

# Current dir by default

my $outdir = ".";

if (exists($args{f})){
    $outfile = $args{f};
}

if (exists($args{d})){
    $outdir = $args{d};
    $outdir =~ s{/\z}{};
}

pmiStart("$outdir/$outfile", 0) >= 0
or die "pmiStart($outfile, 0): " . pmiErrStr(-1) . "\n";


if($label_zone){
    #print "Setting TZ to $zone : $label_zone\n";
    pmiSetTimezone($label_zone) >= 0
    or die "pmiSetTimezone($label_zone): " . pmiErrStr(-1) . "\n";
}

my $inhost = "";
my $outhost = "";

if (exists($args{h})) {
    $outhost = $args{h};
}

if (exists($args{n})) {
    $inhost = $args{n};
}

if ($outhost){
    pmiSetHostname($outhost) == 0
    or die "pmiSetHostname($outhost): ". pmiErrStr(-1) . "\n";
}

my $step_input = 0;
if (exists($args{t})) {
    $step_input = $args{t};
}

my $rrd_dir = $ARGV[0];
#if( $input_method eq "rrdfetch_local" ){
#    $rrd_dir =~ s{/\z}{};
#}elsif ($input_method eq "read_local" ) {
#    $rrd_dir = $rrd_dir."/".$adminnode."/yellowstone/".$inhost;
#}else{
#    print "Unsupport input method, please redefine input_method string to rrdfetch_local or read_local \n";
#    exit 1;
#}
$rrd_dir =~ s{/\z}{};

opendir(RRD_DIR, $rrd_dir) || die "Can't open directory: $rrd_dir\n";

while(my $rrd_file = readdir(RRD_DIR)){
	# Only grab rrd files and only if we have a mapping
	# Skipping metrics we don't know how to convert
    my $locate_file = ($rrd_file =~ /\w+.rrd/ && exists $rrd_pcp_mapping{$rrd_file});
    if( $locate_file ){
		# Debug
        if ( $input_method eq "rrdfetch_local" ){
		my $rrd_file_hash = RRDs::info("$rrd_dir/$rrd_file");
		my $ERR=RRDs::error;
		die "ERROR in RRDs::info($rrd_dir/$rrd_file) : $ERR\n" if $ERR;
        }elsif ($input_method eq "read_local" ) {
                my $rrd_file_hash = $rrd_dir."/".$rrd_file;
                die "$rrd_file_hash not exists!\n" if ! -e $rrd_file_hash;
        }

		#print "\nOpening: $rrd_dir/$rrd_file\n";
		#foreach my $key (keys %$rrd_file_hash){
		#	my $vall = "undef";
		#	print "$key = ";
		#	if( defined $rrd_file_hash->{"$key"} ){
		#		$vall = $rrd_file_hash->{"$key"};
		#	}
		#	print "$vall\n";
		#}

		$rrd_found{"$rrd_file"} = 1;

		# Grab the highest resolution data
		# As returned by RRDs::fetch, step is already : step * pdp_per_row

        #if ( $input_method eq "rrdfetch_local" ){
        #    my ($start,$step,$names,$data) = RRDs::fetch("$rrd_dir/$rrd_file", $starttimearg, $endtimearg, "AVERAGE");
        #}elsif ($input_method eq "read_local" ) {
        #    my ($start,$step,$names,$data) = read_local_rrdfetch_files($rrd_dir, $rrd_file, $starttimearg, $endtimearg, "AVERAGE");
        #    print "$start,$step,$names,$data",$start,$step,$names,$data;
        #}else{
        #    print "Unsupport input method, please redefine input_method string to rrdfetch_local or read_local \n";
        #    exit 1;
        #}

        my ($start,$names,$data) = read_local_rrdfetch_files($rrd_dir, $rrd_file, $starttimearg, $endtimearg, "AVERAGE");
        my $step=$step_input;
 
		# Debug
		#print "Start:       ", scalar localtime($start), " ($start)\n";
		#print "Step size:   $step seconds\n";
		#print "DS names:    ", join (", ", @$names)."\n";
		#print "Data points: ", $#$data + 1, "\n";

		# Regardless of when the samples are recorded (some timestamps might be missing),
		#   we need to know the step size to back convert counters
		#
		# Might be different for each metric
		$rrd_pcp_mapping{$rrd_file}{"stepsize"} = $step;

		# I don't have any files with multiple metrics per file
		# Using the standard mappings above, each "line" should have only one value

		for my $line (@$data) {
                        my @line_content=split /:/, $line;
			$start = $line_content[0];
                        $line_content[1] =~ s/^\s+//; #remove leading spaces
                        #print "line_content[1]=",$line_content[1],"\n";
                        $rrd_stats{$start}{"$rrd_file"} = $line_content[1];
			#for my $val (@$line) {
			#	if( defined $val){ # Last value is almost always NaN
			#		# Add to the hash
			#		$rrd_stats{$start}{"$rrd_file"} = $val;
			#	}
			#}
		}
	}
}

# Add the metrics we have found based on the directory contents

# Keep track of the metrics added, so we don't duplicate source metrics that map to instances in pcp
my %metrics_added;

# Keep track of instances added, so we only add each instance once
my %instances_added;

foreach my $rrd_metric ( sort {lc $a cmp lc $b} keys %rrd_found ){
	# Skip any we have not been configured to convert
	# All should be here, since we check above
	if( exists $rrd_pcp_mapping{$rrd_metric} ){
		# Don't add metrics that are grouped as instances
		if( ! $metrics_added{$rrd_pcp_mapping{$rrd_metric}{"name"}}){
			pmiAddMetric($rrd_pcp_mapping{$rrd_metric}{"name"},
					$rrd_pcp_mapping{$rrd_metric}{"pmid"},
					$rrd_pcp_mapping{$rrd_metric}{"type"},
					$rrd_pcp_mapping{$rrd_metric}{"indom"},
					$rrd_pcp_mapping{$rrd_metric}{"sem"},
					$rrd_pcp_mapping{$rrd_metric}{"units"}) == 0
				or die "pmiAddMetric(".$rrd_pcp_mapping{$rrd_metric}{"name"}.", ...): " . pmiErrStr(-1) . "\n";
			# Keep track of metrics so we don't add more than once if they map to instances
			$metrics_added{$rrd_pcp_mapping{$rrd_metric}{"name"}}=1;
		}
		
		# Add correct instance domains

		if( $rrd_pcp_mapping{$rrd_metric}{"indom"} != PM_INDOM_NULL ){

			# Is this an indom that doesn't map 1:1 from ganglia and we need to duplicate values
			if( defined $rrd_pcp_mapping{$rrd_metric}{"inst_pattern"} ){
				# And we haven't built it already
				if( $rrd_pcp_mapping{$rrd_metric}{'inst_constructed'} == -1){
					my $indom_src = $rrd_pcp_mapping{$rrd_metric}{"inst_src"};
					my $num_inst = 0;
					# Get the first time we see the value we need.  A bit cumbersome
					my $foundit = 0;
					foreach my $timestep ( sort {$a<=>$b} keys %rrd_stats ){
						if( $foundit ){
							last;
						}
						foreach my $metric ( sort {lc $a cmp lc $b} keys %{$rrd_stats{$timestep}} ){
							if( $metric eq $indom_src ){
								my $metric_value = $rrd_stats{$timestep}{$metric};
								if( defined $metric_value ){
									$num_inst = $metric_value;
									$foundit = 1;
									last;
								}
							}
						}
					}
					if( !$foundit ){
						# Can't really recover since there is no instance
						# Maybe, deconfigure ourselves for this metric and log it?
						die "Couldn't find indom information for $rrd_metric\n";
					}
					# Keep track for later
					$rrd_pcp_mapping{$rrd_metric}{'inst_constructed'} = $num_inst;

					foreach my $iid (0..$num_inst-1){
						my $inst_spec = $rrd_pcp_mapping{$rrd_metric}{"inst_pattern"};
						my $iname_spec = $rrd_pcp_mapping{$rrd_metric}{"iname_pattern"};
						my $inst = sprintf($inst_spec, $iid);
						my $iname = sprintf($iname_spec, $iid);


						my $indomID;
						$indomID = "$rrd_pcp_mapping{$rrd_metric}{'indom'}" . $inst;

						#print "Adding constructed inst: $inst $iname for $rrd_pcp_mapping{$rrd_metric}{'name'}\n";

						if( ! $instances_added{$indomID} ){
							pmiAddInstance($rrd_pcp_mapping{$rrd_metric}{"indom"},
									$iname,
									$inst) >= 0
								or die "pmiAddInstance " . $rrd_pcp_mapping{$rrd_metric}{'name'} . $rrd_pcp_mapping{$rrd_metric}{'iname'} . pmiErrStr(-1) . "\n";
							$instances_added{$indomID}=1;
						}

					}
				}
			}
			else{
				# Regular instance mapping
				
				# An identifier that we can store for indom/instance ID
				my $indomID;
				$indomID = "$rrd_pcp_mapping{$rrd_metric}{'indom'}" . $rrd_pcp_mapping{$rrd_metric}{'inst'}; 

				if( ! $instances_added{$indomID} ){
					pmiAddInstance($rrd_pcp_mapping{$rrd_metric}{"indom"},
							$rrd_pcp_mapping{$rrd_metric}{"iname"},
							$rrd_pcp_mapping{$rrd_metric}{"inst"}) >= 0
						or die "pmiAddInstance " . $rrd_pcp_mapping{$rrd_metric}{'name'} . $rrd_pcp_mapping{$rrd_metric}{'iname'} . pmiErrStr(-1) . "\n";
					$instances_added{$indomID}=1;
				}
			}
		}
	}
	else{
		warn "$rrd_metric not configured for conversion\n";
	}
}

# TODO, get handles instead ??

# Loop through by time
foreach my $timestep ( sort {$a<=>$b} keys %rrd_stats ){
	foreach my $metric ( sort {lc $a cmp lc $b} keys %{$rrd_stats{$timestep}} ){
		
		#print "pmiPutValue : " . $rrd_pcp_mapping{$metric}{"name"} . " : " . $metric . " : " . "$rrd_stats{$timestep}{$metric}" . "\n";

		my $metric_value = 0;
		if( exists $rrd_pcp_mapping{$metric}{"conv_fcn"} ){
			$metric_value = $rrd_pcp_mapping{$metric}{"conv_fcn"}->($metric, $rrd_stats{$timestep}{$metric}, $timestep);
		}
		else{
			$metric_value = $rrd_stats{$timestep}{$metric};
		}

		if( $rrd_pcp_mapping{$metric}{"indom"} == PM_INDOM_NULL){
			# No indom
                        # 4011 records show "pmiPutValue failed NULL indom" in 20160727
                        #print "metric_value=",$metric_value," rrd_pcp_mapping{metric}{name}=",$rrd_pcp_mapping{$metric}{"name"}," metric=", $metric, "\n";
			my $sts = pmiPutValue($rrd_pcp_mapping{$metric}{"name"}, "", $metric_value);
			if( $sts < 0 ){
				die "pmiPutValue failed NULL indom : " . pmiErrStr($sts) . "\n";
			}
		}
		elsif(!defined $rrd_pcp_mapping{$metric}{"inst_pattern"}){
			# Regular Indom
                        #print "metric_value=",$metric_value," rrd_pcp_mapping{metric}{name}=",$rrd_pcp_mapping{$metric}{"name"}," metric=", $metric, "\n";
			my $sts = pmiPutValue($rrd_pcp_mapping{$metric}{"name"}, $rrd_pcp_mapping{$metric}{"iname"}, $metric_value);
			if( $sts < 0 ){
                                die "pmiPutValue failed indom : " . pmiErrStr($sts) . "\n";
                        }
		}
		else{
			# Constructed indom
			my $num_inst = $rrd_pcp_mapping{$metric}{'inst_constructed'};
                        #print "metric=",$metric;
                        #print "num_inst=",$num_inst;

			foreach my $iid(0..$num_inst-1){

				my $iname_spec = $rrd_pcp_mapping{$metric}{"iname_pattern"};
				my $iname = sprintf($iname_spec, $iid);
				
                                #print "metric_value=",$metric_value," rrd_pcp_mapping{metric}{name}=",$rrd_pcp_mapping{$metric}{"name"}," metric=", $metric, "\n";
				my $sts = pmiPutValue($rrd_pcp_mapping{$metric}{"name"}, $iname, $metric_value);
				if( $sts < 0 ){
					die "pmiPutValue failed constructed indom : " . pmiErrStr($sts) . "\n";
				}
			}
			
		}
	}
	pmiWrite($timestep, 0) >= 0 or die "pmiWrite failed :" . pmiErrStr(-1) . "\n";
}

pmiEnd() >= 0 or die "pmiEnd failed\n";
