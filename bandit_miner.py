from pattern_miner import PatternChunk, MiningPattern
import re


IBOX=">> Issue: \[(?P<issue_num>\w+)\:(?P<issue_name>[\w\_\d]+)\]"
IDESC="(?P<issue_desc>[^\n]+)\n"
SEVCON="\s+Severity\:\s(?P<severity>\w+)\s+Confidence\:\s(?P<confidence>\w+)\n"
LOC="\s+Location\:(?P<file_name>[^\:]+)\:(?P<line_num>\d+)\n"
CODE="(?P<code>\d+\t[^\n]*\n)"
END_BUFF="\n--------------------------------------------------"

bandit_pattern = MiningPattern([
    PatternChunk(IBOX, is_repeating=False, optional=False),
    PatternChunk(IDESC, is_repeating=False, optional=False),
    PatternChunk(SEVCON, is_repeating=False, optional=False),
    PatternChunk(LOC, is_repeating=False, optional=False),
    PatternChunk(CODE, is_repeating=True, optional=False),
])


bandit_regex = dict(
    issuere=re.compile(IBOX+IDESC+SEVCON+LOC+CODE+END_BUFF, re.M)
)

#^>> Issue: \[(?P<issue_num>\w+)\:(?P<issue_name>[\w\_\d]+)\](?P<issue_desc>[^\n]+)\n\s+Severity\:\s(?P<severity>\w+)\s+Confidence\:\s(?P<confidence>\w+)\n\s+Location\:(?P<file_name>[^\:]+)\:(?P<line_num>\d+)\n(?P<code>(\d+\t[^\n]*\n){3,})\n--------------------------------------------------
