################################################
# Test the LOF sieve tool with snpEff
################################################
set -e
echo "    sieve.t01...\c"
echo "chrom	start	end	ref	alt	highest_impact	aa_change	var_trans_pos	trans_aa_length	var_trans_pct	sample	genotype	gene	transcript	trans_type
chr1	1219381	1219382	C	G	stop_gained	S29*	29	138	0.210144927536	1719PC0012	C/G	SCNN1D	ENST00000470022	protein_coding" > obs

gemini lof_sieve test.query.db > exp
check obs exp
rm obs exp
################################################
# Test the LOF sieve tool with VEP
################################################
echo "    sieve.t02...\c"
echo "chrom	start	end	ref	alt	highest_impact	aa_change	var_trans_pos	trans_aa_length	var_trans_pct	sample	genotype	gene	transcript	trans_type
chr1	1219381	1219382	C	G	stop_gained	S/*	29	29/139	0.208633093525	1719PC0012	C/G	SCNN1D	ENST00000470022	protein_coding" > obs

gemini lof_sieve test.query.vep.db > exp
check obs exp
rm obs exp

echo "    sieve.t03...\c"
echo "chrom	start	end	ref	alt	highest_impact	aa_change	var_trans_pos	trans_aa_length	var_trans_pct	sample	genotype	gene	transcript	trans_type
chr19	490039	490040	G	A	splice_donor_variant	None	None	-/163	/	DNA_A02	G/A	MADCAM1	ENST00000587541	protein_coding" > exp
gemini lof_sieve test-lof.db > obs
check obs exp
