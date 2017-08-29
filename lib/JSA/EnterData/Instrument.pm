package JSA::EnterData::Instrument;

use strict;
use warnings;

my %_default = (
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

1;
