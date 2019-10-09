#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;

use Data::Dumper;
use Mojo::Redis;
use Time::HiRes qw(time);
use Mojo::IOLoop::Signal;

use constant {
    STREAM_CHAN		 => 'someshit',
    STREAM_ID_FILE	 => 'stream_id',
    DFLT_START_STREAM_ID => 0,
};

my $mr = Mojo::Redis->new;
my $redc = $mr->db;

my $streamID = my $initStreamID =
    ( -f STREAM_ID_FILE ) 
        ? do {
            open my $fh, '<', STREAM_ID_FILE;
            chomp($_ = <$fh>);
            /^\d+-\d+$/ ? $_ : DFLT_START_STREAM_ID
          }
        : DFLT_START_STREAM_ID;

Mojo::IOLoop->recurring(0.5 => sub {
    $redc->xadd_p(STREAM_CHAN, '*', 'ts' => $cur_ts = time)
         ->then(sub { say 'xadd: ts ' . $cur_ts })
});

my $xp = XRead::Printer->new;
Mojo::IOLoop->recurring(0.2 => sub {
    $redc->xread_p('STREAMS', STREAM_CHAN, $streamID)
         ->then(sub {
            my $entries = eval { $_[0][0][1] };
            $xp->next;
            $xp->tell_delta;
            unless ( defined($entries) and @{$entries} ) {
                $xp->tell('no new entries?')
            } else {
                $streamID = $entries->[$#{$entries}][0];
                $xp->tell('data: %s', Dumper( turn2hr( $entries, 1 ) ));
            }
        })
        ->catch(sub {
            say 'Oh, f*ck, exception!! ' . Dumper(\@_)
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
        open my $fh, '>', STREAM_ID_FILE;
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

sub next { $_[0][CNT]++ }

sub tell {
    my $me = shift;
    printf $me->[STR] . ' #%d: ' . shift . "\n", $me->[CNT], @_;
}

sub tell_delta {
    my $me = shift;
    my $prv_ts = $me->[TS];
    $me->tell('after %s ms', int(1000 * (($me->[TS] = time) - $prv_ts)) / 1000 );
}
