#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;
use constant {
    STREAM_CHAN => 'someshit',
    STREAM_ID_FILE => 'stream_id',
    DFLT_START_STREAM_ID => 0,
};
use constant DONT_TURN_TO_HASH => 1;
use Data::Dumper;
use AnyEvent;
use Redis::Fast;
use Time::HiRes qw(time);

my $redc = Redis::Fast->new;
my $cv = AnyEvent->condvar;
my %aeh;

my $streamID = my $initStreamID =
    ( -f STREAM_ID_FILE ) 
        ? do {
            open my $fh, '<', STREAM_ID_FILE;
            chomp($_ = <$fh>);
            /^\d+-\d+$/ ? $_ : DFLT_START_STREAM_ID
          }
        : DFLT_START_STREAM_ID;

$aeh{'xadd'} = 
    AnyEvent->timer(
        after => 0,
        interval => 0.2,
        cb => sub {
            $redc->xadd(STREAM_CHAN, '*', 'ts' => time());
            say 'xadd: did it!';
        }
    );

$aeh{'xread'} = 
    AnyEvent->timer(
        after => 0.3,
        interval => 0.8,
        cb => sub {
            my $entries = $redc->xread('STREAMS', STREAM_CHAN, $streamID)->[0][1];
            unless ( @{$entries} ) {
                say 'xread: no new entries?'
            } else {
                $streamID = $entries->[$#{$entries}][0];
                print 'xread: data ' . Dumper( turn2hr( $entries, DONT_TURN_TO_HASH ) );
            }
        }
    );
    
$aeh{'keyb_int'} = AnyEvent->signal(
    signal => 'INT',
    cb => sub {
        say 'Received keyboard interrupt, exiting EVLoop...';
        $cv->send
    }
);

$cv->recv;

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
        open my $fh, '>', STREAM_ID_FILE;
        print $fh $streamID;
        close $fh;
    }
}
