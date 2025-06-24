CREATE TABLE hep_singleMu AS SELECT * FROM READ_PARQUET('s3://duckdb-blobs/data/realnest/Run2012B_SingleMu_restructured_1000.parquet');

CREATE FUNCTION Pi() AS (ACOS(-1));

CREATE FUNCTION FMod2Pi(x) AS (FMod(x, 2 * Pi()));

CREATE FUNCTION Square(x) AS (x*x);

CREATE FUNCTION DeltaPhi(p1, p2) AS (
    CASE
    WHEN FMod2Pi(p1['phi'] - p2['phi']) < -Pi() THEN FMod2Pi(p1['phi'] - p2['phi']) + 2 * Pi()
    WHEN FMod2Pi(p1['phi'] - p2['phi']) >  Pi() THEN FMod2Pi(p1['phi'] - p2['phi']) - 2 * Pi()
    ELSE FMod2Pi(p1['phi'] - p2['phi'])
    END
);

CREATE FUNCTION DeltaR(p1, p2) AS (SQRT(Square(p1['eta'] - p2['eta']) + Square(DeltaPhi(p1, p2))));

CREATE FUNCTION RhoZ2Eta(Rho, Z) AS (log(Z/Rho + sqrt(Z/Rho * Z/Rho + 1.0)));

CREATE OR REPLACE FUNCTION PtEtaPhiM2PxPyPzE(pepm) AS
 {'x': pepm['pt'] * cos(pepm['phi']),
    'y': pepm['pt'] * sin(pepm['phi']),
    'z': pepm['pt'] * sinh(pepm['eta']),
    't': sqrt((pepm['pt'] * cosh(pepm['eta']))*(pepm['pt'] * cosh(pepm['eta'])) + pepm['mass'] * pepm['mass'])
 };

CREATE OR REPLACE FUNCTION PxPyPzE2PtEtaPhiM(xyzt) AS
 {'pt': sqrt(xyzt['x']*xyzt['x'] + xyzt['y']*xyzt['y']),
    'eta': RhoZ2Eta(sqrt(xyzt['x']*xyzt['x'] + xyzt['y']*xyzt['y']), xyzt['z']),
    'phi': CASE WHEN (xyzt['x'] = 0.0 AND xyzt['y'] = 0.0) THEN 0 ELSE atan2(xyzt['y'], xyzt['x']) END,
    'mass': sqrt(xyzt['t']*xyzt['t'] - xyzt['x']*xyzt['x'] - xyzt['y']*xyzt['y'] - xyzt['z']*xyzt['z'])
 };

CREATE OR REPLACE FUNCTION AddPxPyPzE2( xyzt1, xyzt2) AS
    {'x' : xyzt1['x'] + xyzt2['x'],
    'y' : xyzt1['y'] + xyzt2['y'],
    'z' : xyzt1['z'] + xyzt2['z'],
    't' : xyzt1['t'] + xyzt2['t']
    };

CREATE OR REPLACE FUNCTION AddPxPyPzE3(xyzt1, xyzt2, xyzt3) AS
 AddPxPyPzE2(xyzt1, AddPxPyPzE2(xyzt2, xyzt3)
 );

CREATE OR REPLACE FUNCTION AddPtEtaPhiM2(pepm1, pepm2) AS
    AddPxPyPzE2(
      PtEtaPhiM2PxPyPzE(pepm1),
      PtEtaPhiM2PxPyPzE(pepm2)
    );

CREATE OR REPLACE FUNCTION AddPtEtaPhiM3(
   pepm1,
   pepm2 ,
   pepm3) AS
 PxPyPzE2PtEtaPhiM(
    AddPxPyPzE3(
      PtEtaPhiM2PxPyPzE(pepm1),
      PtEtaPhiM2PxPyPzE(pepm2),
      PtEtaPhiM2PxPyPzE(pepm3)
    )
);

CREATE OR REPLACE FUNCTION HistogramBinHelper(
   value, lo, hi, bin_width) AS (
 FLOOR((
   CASE
     WHEN value < lo THEN lo - bin_width / 4
     WHEN value > hi THEN hi + bin_width / 4
     ELSE value
   END - FMod(lo, bin_width)) / bin_width) * bin_width
     + bin_width / 2 + FMod(lo, bin_width)
);

CREATE OR REPLACE FUNCTION HistogramBin(
   value, lo, hi, num_bins) AS (
 HistogramBinHelper(value, lo, hi, (hi - lo) / num_bins)
);