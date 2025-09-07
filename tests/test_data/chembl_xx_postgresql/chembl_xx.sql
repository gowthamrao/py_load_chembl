-- A highly simplified schema for testing purposes
CREATE TABLE molecule_dictionary (
    molregno     integer NOT NULL,
    chembl_id    varchar(20) NOT NULL,
    pref_name    varchar(255),
    molecule_type varchar(30)
);

INSERT INTO molecule_dictionary VALUES (1, 'CHEMBL1', 'ASPIRIN', 'Small Molecule');
INSERT INTO molecule_dictionary VALUES (2, 'CHEMBL2', 'IBUPROFEN', 'Small Molecule');
