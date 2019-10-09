#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;

use Data::Dumper;
use Mojo::Redis;
use Time::HiRes qw(time);
use Mojo::IOLoop::Signal;
use Getopt::Std qw(getopts);

use constant {
    DFLT_STREAM_CHAN		=> 'someshit',
    DFLT_STREAM_ID_FILE		=> 'stream_id',
    DFLT_START_STREAM_ID	=> 0,
    DFLT_ADD_EVERY		=> 0.5,
    DFLT_READ_EVERY             => 0.2,
};

getopts 'A:R:n:f:i:', \my %opts;
my $streamName		= $opts{'n'} // DFLT_STREAM_CHAN;
my $streamIdFile        = $opts{'f'} // DFLT_STREAM_ID_FILE;
my $addEvery 		= $opts{'A'} // DFLT_ADD_EVERY;
my $readEvery           = $opts{'R'} // DFLT_READ_EVERY;
index($streamIdFile, '{{STREAM}}') > 0 and $streamIdFile =~ s%\{\{STREAM\}\}%${streamName}%g;

my $streamID = my $initStreamID =
    ( defined($opts{'i'}) && $opts{'i'} =~ /^\d+(?:-\d+)?$/ )
        ? $opts{'i'}
        : ( -f $streamIdFile ) 
            ? do {
                open my $fh, '<', $streamIdFile or die "Cant read ${streamIdFile}: $@";
                chomp($_ = <$fh>);
                /^\d+-\d+$/ ? $_ : DFLT_START_STREAM_ID
              }
            : DFLT_START_STREAM_ID;
        
my $mr = Mojo::Redis->new;
my $redc = $mr->db;

my $xr = XRead::Printer->new;
my $xa = XRead::Printer->new('xadd');

Mojo::IOLoop->recurring($addEvery => sub {
    my $cur_ts = time;
    $redc->xadd_p($streamName, '*', 'ts' => $cur_ts)
         ->then(sub { 
             $xa->next('tell' => 'pushed record with ts=%g', $cur_ts)
         })
         ->catch(sub {
             $xa->next('tell' => 'CATCHED: %s', $_[0])
         })
});

Mojo::IOLoop->recurring($readEvery => sub {
    $redc->xread_p('STREAMS', $streamName, $streamID)
         ->then(sub {
            my $entries = eval { $_[0][0][1] };
            $xr->next('tell_delta');
            unless ( defined($entries) and @{$entries} ) {
                $xr->tell('no new entries?')
            } else {
                $streamID = $entries->[$#{$entries}][0];
                $xr->tell('data: %s', Dumper( turn2hr( $entries, 1 ) ))
            }
        })
        ->catch(sub {
            $xr->next('tell' => 'CATCHED: %s', Dumper(\@_))
        })
});

Mojo::IOLoop::Signal->on('INT' => sub {
    say 'Received keyboard interrupt, exiting EVLoop...';
    Mojo::IOLoop->stop;
});

Mojo::IOLoop->start;

sub turn2hr {
    ref( $_[0] )
    ? ( ref( $_[0][0] ) or $_[1] )
            ? [ map turn2hr($_), @{$_[0]} ]
            : do {
                my $c = 0;
                +{ map {
                    $c++ & 1
                        ? ref($_)
                            ? turn2hr($_)
                            : $_
                        : $_
                } @{$_[0]} }
              }
    : $_[0]
}

END {
    if ( defined($streamID) and $streamID ne $initStreamID ) {
        open my $fh, '>', $streamIdFile;
        print $fh $streamID;
        close $fh;
    }
}

package XRead::Printer;
use Time::HiRes qw(time);
use constant {
    CNT => 0,
    STR => 1,
    TS	=> 2,
    DFLT_STR => 'xread',
};

sub new {
    bless [0, $_[1] // DFLT_STR, time], ref($_[0]) || $_[0]
}

sub next { 
    my $me = shift;
    my $cnt = ++$me->[CNT];
    return $cnt unless @_;
    my $methodName = shift;
    if (defined($methodName) and ! ref($methodName) and my $methodRef = $me->can($methodName)) {
        unshift @_, $me;
        goto &{$methodRef}
    }
}

sub tell {
    my $me = shift;
    printf $me->[STR] . ' #%d: ' . shift . "\n", $me->[CNT], @_;
}

sub tell_delta {
    my $me = shift;
    my $prv_ts = $me->[TS];
    $me->tell('after %s ms', int(1000 * (($me->[TS] = time) - $prv_ts)) );
}
