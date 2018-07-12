=head1 NAME

JSA::DB::MongoDB - Interface module for storing JSA data in MongoDB

=cut

package JSA::DB::MongoDB;

use strict;

use Astro::FITS::Header;
use Astro::FITS::Header::Item;
use Astro::FITS::Header::NDF;
use boolean;
use BSON;
use BSON::Types ':all';
use DateTime::Format::ISO8601;
use Digest::MD5;
use File::Spec;
use IO::File;
use MongoDB;

our $valid_filename = qr/[a-z0-9]+\.[a-z0-9]+/;
our $valid_date = qr/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?/;

=head1 CONSTRUCTOR

Prepares a MongoDB::MongoClient connection.

    my $db = new JSA::DB::MongoDB();

=cut

sub new {
    my $class = shift;

    my $self = {
        client => MongoDB->connect(
            'mongodb://localhost', {
            bson_codec => new BSON(
                ordered => 1,
                wrap_strings => 1,
                wrap_numbers => 1),
        }),
    };

    bless $self, $class;

    return $self;
}

=head1 METHODS

=over 4

=item put_raw_file

Add (or update) information about a raw file to the database.

Options:

=over 4

=item file

(Path) name of a local file to be processed.

=back

=cut

sub put_raw_file {
    my ($self, %opt) = @_;

    my ($ident, $info);

    if (exists $opt{'file'}) {
        ($ident, $info) = prepare_file_record_local($opt{'file'});
    }
    else {
        die 'Unknown put_file operation';
    }

    $self->{'client'}->ns('jcmt.raw_file')->update_one(
        $ident,
        {
            '$set' => $info,
            '$currentDate' => {'modified' => true},
        },
        {
            upsert => true,
        },
    );
}

=item get_raw_header

Retrieves raw file headers from the database.

Returns a reference to an array of Astro::FITS::Header objects.

Options:

=over 4

=item file

(Base) name of file to search for.

=item obsid

OBSID header value.

=back

=cut

sub get_raw_header {
    my ($self, %opt) = @_;

    my %query = ();

    if (exists $opt{'obsid'}) {
        $query{'header.OBSID'} = $opt{'obsid'};
    }

    if (exists $opt{'file'}) {
        $query{'_id'} = $opt{'file'};
    }

    my $cursor = $self->{'client'}->ns('jcmt.raw_file')->find(\%query);

    my $query_result = $cursor->result();

    my @result = ();

    while (my @batch = $query_result->batch()) {
        foreach my $doc (@batch) {
            push @result, bson_to_header($doc->{'header'});
        }
    }

    return \@result;
}

=back

=head1 FUNCTIONS

=over 4

=item prepare_file_record_local($filename)

Prepares database information for a file available locally.

This computes the MD5 sum of the file and reads its header
before calling L<prepare_file_record> to generate the database
information.

=cut

sub prepare_file_record_local {
    my $file = shift;

    my (undef, undef, $basename) = File::Spec->splitpath($file);

    my $hdr = new Astro::FITS::Header::NDF(File => $file);

    my $ctx = new Digest::MD5();
    my $fh = new IO::File($file, 'r');
    $ctx->addfile($fh);
    $fh->close();
    my $md5sum = $ctx->hexdigest();

    return prepare_file_record($basename, $hdr, $md5sum);
}


=item prepare_file_record($basename, $header, $md5sum)

Prepares information for the database record about a file.

Returns a list consisting of two documents: one which identifies the
file record and a second containing the actual information.  These can
be used in the "query" and "update" parts of an update operation.

    my ($identification, $file_information) = prepare_file_record(...);

=cut

sub prepare_file_record {
    my $basename = shift;
    my $hdr = shift;
    my $md5sum = shift;

    die "File base name $basename is not valid"
        unless $basename =~ $valid_filename;

    return
        bson_doc(
            _id => $basename,
        ),
        bson_doc(
            md5sum => $md5sum,
            header => header_to_bson($hdr),
        );
}


=item header_to_bson($header)

Convert an Astro::FITS::Header object to a BSON document.

The intention is to preseve the header in as close to its original
format as possible.

=over 4

=item *

Comments and blank headers are skipped.

=item *

Strings which look like datetimes are converted to DateTime objects in UTC
unless the keyword is "HSTSTART" or "HSTEND".

=item *

Numbers and logical fields are wrapped in BSON type wrappers.

=back

=cut

sub header_to_bson {
    my $header = shift;

    my @doc = ();

    foreach my $item ($header->allitems()) {
        my $type = $item->type();

        if ($type eq 'COMMENT' or $type eq 'BLANK') {
            next;
        }

        my $keyword = $item->keyword();
        my $value = $item->value();
        my $hdr = undef;

        if ($type eq 'STRING') {
            if (($value =~ $valid_date) and not ($keyword eq 'HSTSTART' or $keyword eq 'HSTEND')) {
                my $dt = DateTime::Format::ISO8601->parse_datetime($value);
                $dt->set_time_zone('UTC');
                $hdr = $dt;
            }
            else {
                $hdr = bson_string($value);
            }
        }
        elsif ($type eq 'INT') {
            if ($value > 2147483647 or $value < -2147483648) {
                $hdr = bson_int64($value);
            }
            else {
                $hdr = bson_int32($value);
            }
        }
        elsif ($type eq 'FLOAT') {
            $hdr = bson_double($value);
        }
        elsif ($type eq 'LOGICAL') {
            $hdr = $value ? true : false;
        }
        elsif ($type eq 'UNDEF') {
            $hdr = undef;
        }
        else {
            die "Unexpected type $type for FITS keyword $keyword";
        }

        push @doc, $keyword, $hdr;
    }

    return bson_doc(@doc);
}

=item bson_to_header($document)

Convert a BSON document back to an Astro::FITS::Header object.

B<Note:> this expects that numbers and strings are found within BSON type
wrappers.  (I.e. the BSON document should have been decoded with the
C<wrap_strings> and C<wrap_numbers> attributes enabled.)

=cut

sub bson_to_header {
    my $doc = shift;

    my @cards = ();

    while (my ($key, $val) = each %$doc) {
        my $type = undef;

        if (UNIVERSAL::isa($val, 'BSON::Time')) {
            $val = $val->as_iso8601();
            $val =~ s/Z$//;
            $type = 'STRING';
        }
        elsif (UNIVERSAL::isa($val, 'BSON::Double')) {
            # Unfortunately Astro::FITS::Header doeesn't always include
            # a decimal point in the value for this type.
            $type = 'FLOAT';
        }
        elsif (UNIVERSAL::isa($val, 'BSON::Int32')
                or UNIVERSAL::isa($val, 'BSON::Int64')) {
            $type = 'INT';
        }
        elsif (UNIVERSAL::isa($val, 'BSON::String')) {
            $type = 'STRING';
        }
        elsif (UNIVERSAL::isa($val, 'boolean')) {
            $type = 'LOGICAL';
            $val = $val ? 1 : 0;
        }
        elsif (not defined $val) {
            $type = 'UNDEF';
        }
        else {
            die "Could not determine type for keyword $key";
        }

        my $card = new Astro::FITS::Header::Item(
            Keyword => $key,
            Value => $val,
            Type => $type,
        );

        # The POD for Astro::FITS::Header::Item::new says that it will return
        # undef if the information is insufficient, but it's not clear that it
        # ever actually does.
        die "Could not construct header item for keyword $key"
            unless defined $card;

        push @cards, $card;
    }

    return new Astro::FITS::Header(Cards => \@cards);
}

1;

__END__

=back

=cut
