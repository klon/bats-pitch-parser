bats-pitch-parser
=================

A message parser for BATS Multicast PITCH protocol implemented in Python. The parser currently 
only supports a subset of the specification relating to the order book.

See specification at http://cdn.batstrading.com/resources/participant_resources/BATS_Europe_MC_PITCH_Specification.pdf

To run the sample code use:

	python pitch.py < testdata/pitch
	
	python order_analysis.py < testdata/pitch