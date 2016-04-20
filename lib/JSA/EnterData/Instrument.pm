package JSA::EnterData::Instrument;

use strict;
use warnings;

our $DEBUG;

my %_default = (
    'debug' => 0,
);

sub new {
    my ($class, %args) = @_;

    my $obj = bless {}, $class;

    foreach my $k (keys %_default) {
        if (my $sub = $obj->can($k)) {
            $obj->$sub(exists $args{$k} ? $args{$k} : $_default{$k});
        }
    }

    return $obj;
}

# It is in addition to direct access to
# $JSA::EnterData::Instrument::DEBUG.
sub debug {
    my $self = shift @_;

    return $DEBUG
        unless scalar @_;

    $DEBUG = !! $_[0];
    return;
}

1;

=pod


=cut
