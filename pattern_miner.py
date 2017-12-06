from functools import wraps
import logging
import pandas as pd
import re
import sys

def updt(total, progress):
    """
    Displays or updates a console progress bar.

    Original source: https://stackoverflow.com/a/15860757/1391441
    This version is from stack overflow user Gabriel:
        https://stackoverflow.com/users/1391441/gabriel
    """
    barLength, status = 20, ""
    progress = float(progress) / float(total)
    if progress >= 1.:
        progress, status = 1, "\r\n"
    block = int(round(barLength * progress))
    text = "\r[{}] {:.2f}% {}".format(
        "#" * block + "-" * (barLength - block), round(progress * 100, 2),
        status)
    sys.stdout.write(text)
    sys.stdout.flush()


class PatternChunk(object):
    def __init__(self, pattern, is_repeating, optional):
        self.repeats = is_repeating
        self.optional = optional
        self.pattern = re.compile(pattern) if type(pattern) == str else pattern

    def match(self, line):
        m = self.pattern.match(line)
        if m:
            return m, line[m.span()[1]:]
        else:
            return None, line

    def __repr__(self):
        rep = f"<PatternChunk {repr(self.pattern.pattern)}"
        if self.optional:
            rep += " OPTIONAL "
        if self.repeats:
            rep += " REPEATS "
        return rep.strip() + ">"


class MiningPattern(object):
    def __init__(self, chunks):
        self.chunks = chunks
        self.pattern_begun = False
        self.errors = []
        self.skipped = []
        self.logger = None

    def inject_logger(self, logger):
        self.logger = logger
    
    def _match_chunk(self, start, chunk, line, i, in_prog=0):
        presult = None

        self.logger.debug(f"On chunk: '{chunk}'")
        m, line = chunk.match(line)
        if m:
            if not self.pattern_begun:
                self.logger.debug("Pattern started")
                self.pattern_begun = True
            self.logger.debug(f"Matched chunk '{chunk}' on {line}")
            if chunk.repeats:
                d = dict()
                for k,v in m.groupdict().items():
                    d[k+f"_{in_prog}"] = v
                return self._return_result(d, start, i), line
            else:
                return self._return_result(m.groupdict(), start, i), line

        elif chunk.optional and self.pattern_begun:
            self.logger.debug(f"Incremening chunk_id to '{chunk_id}'")
            if len(self.chunks) == chunk_id:
                self.logger.debug(f"No chunks left, returning {presult}")
                return presult, line
        elif not self.pattern_begun:
            msg = "Pattern has not begun, and no match."
            self.logger.debug(msg + f" Returning {presult}.")
            return presult, line
        elif chunk.repeats:
            self.logger.debug(f"Chunk {chunk} at end.")
            return None, line
        else:
            return False, line

    def _return_result(self, md, start, i):
        presult = md.copy()
        if start == None:
            presult["start"] = i
        presult["end"] = i
        self.logger.debug(f"Returning result '{presult}'")
        return presult

    def _cascade_chunks(self, line):
        #TODO: Figure this shit out
        chunks = self.chunks[1:]
        while chunks:
            chunk = chunks.pop(0)
            self.logger.debug(f"Trying chunk: {chunk}")
            if chunk.match(line):
                self.chunks = chunks
                return self._return_result(chunk.match(line).groupdict())
        return None
    
    def _reset_pattern(self):
        self.pattern_begun = False

    def match(self, lines, start=None):
        presults = dict(start=start, end=0)
        in_prog = 0
        chunks = self.chunks.copy()
        while chunks and lines:
            i, line = lines.pop(0)
            self.logger.debug(f"Popped line #{i}: {repr(line)}")
            while line:
                self.logger.debug(f"Running on line {repr(line)}...")
                if len(chunks) == 0:
                    self.logger.debug(f"Out of chunks...breaking from {repr(line)}")
                    break
                else:
                    chunk = chunks[0]
                    self.logger.debug("...with chunk: {repr(chunk)}")
                presult, line = self._match_chunk(
                    presults.get("start"), chunk, line, i, in_prog
                )
                if presult:
                    if chunks[0].repeats:
                        self.logger.debug(
                            f"Setting in_prog from {in_prog} to {in_prog+1}"
                        ) 
                        in_prog += 1
                    else:
                        self.logger.debug(f"Popping chunk: {chunks[0]}")
                        chunks.pop(0)
                    presults.update(presult)
                    presults["end"] += 1
                elif presult == False:
                    self.logger.debug(
                        f"Could not match {line} with {chunk}"
                    )
                    self.errors.append((line, chunk))
                    return False
                else:
                    if chunks[0].repeats:
                        in_prog = 0
                        msg = "Encountered end of repeating chunk. "
                        self.logger.debug(msg+f"Popping chunk: {chunks[0]}")
                        chunks.pop(0)
                        break
                    elif not self.pattern_begun:
                        self.logger.debug(
                            "Pattern has not begun, so skipping line."
                        )
                        break
                    else:
                        self.logger.warning("Unexpected condition in match()")            
        
        if chunks and not lines and self.pattern_begun:
            raise IndexError(
                f"There are '{len(self.chunks)}' chunks and no lines left."
            )
        else:
            self.logger.debug("Pattern matched. Reseting pattern")
            self._reset_pattern()
            return presults
            

class Miner(object):
    def __init__(self, pattern, logformat=None):
        self._current_doc = []
        self.mined_store = dict(
            _meta=dict(
                records=0,
                indices=[]
            )
        )

        self.pattern = pattern

        if not logformat:
            self.logger = self.establish_logger()
        else:
            self.logger = self.establish_logger(**logformat)
        self.pattern.inject_logger(self.logger)

    @staticmethod
    def establish_logger(format=None, level=logging.INFO, 
                         filename="pattern_miner.log", filemode='w'):
        if not format:
            fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format, level=level,
                            filename=filename, filemode=filemode)
        return logging.getLogger(__name__)  

    def __len__(self):
        return self.mined_store['_meta']['records']

    def _prep_doc(self, fname):
        if type(fname) == list:
            last = 0
            self._current_doc = []
            while fname:
                curdoc = fname.pop(0)
                self.logger.info(f"Loading '{curdoc}'...")                      
                with open(curdoc, 'r', errors="replace") as f:
                    self._current_doc.extend(
                        (i+last,l) for i,l in enumerate(f.readlines())
                    )
                    last = len(self._current_doc)
                self.logger.info(f"Loaded {fname} with '{last}' records.")
        else:
            with open(fname, 'r', errors="replace") as f:
                self._current_doc = [(i,l) for i,l in enumerate(f.readlines())]

    def modify_mined_store(method):
        @wraps(method)
        def _impl(self, *args, **kwargs):
            di = kwargs.get('doc_index', "")
            res = None
            if not di:
                self.logger.debug(
                    f"No 'doc_index' key in kwargs; kwargs='{kwargs}'"
                )
                # TODO: Note that 'doc_index' is expected to be first arg
                # if not in kwargs
                di = args[0]
                if not di:
                    self.logger.warning("Couldn't find index in args or kwargs")
            if di:
                self.logger.debug(f"Modifying index for '{di}'")
                self.mined_store['_meta']['indices'].append(di)
            try:
                res = method(self, *args, **kwargs)
            except:
                self.logger.exception("Uncaught exception")
                if di in self.mined_store['_meta']['indices']:
                    self.mined_store['_meta']['indices'].remove(di)
                raise
            self.mined_store['_meta']['records'] = sum([
                len(self.mined_store[k])\
                for k in self.mined_store['_meta']['indices']\
                if k in self.mined_store
            ])
            return res
        return _impl        

    @modify_mined_store
    def mine_document(self, doc_index, fname, log_type=None):
        self._prep_doc(fname)
        if not doc_index in self.mined_store:
            self.mined_store[doc_index] = []
        lines = self._current_doc
        total = len(lines)
        start = None
        ind = None
        line = None
        while lines:           
            pres = self.pattern.match(lines, start)
            self.logger.debug(
                f"Appending result '{pres}' to mined_store '{doc_index}'"
            )
            if pres:
                self.mined_store[doc_index].append(pres)
                self.logger.debug(
                    f"Moving lines from index '{start}' to "
                    f"'{pres.get('end') + 1}'"
                )
                start = pres.get("end") + 1
                updt(total, start)

    @modify_mined_store
    def offload_document_into_dataframe(self, doc_index, popdata=True):
        df = pd.DataFrame(self.mined_store[doc_index])
        if popdata:
            self.logger.info(f"Removing index '{doc_index}'")
            del self.mined_store[doc_index]
            self.mined_store['_meta']['indices'].remove(doc_index)
            self.logger.info(
                f"Remaining indices are {self.mined_store['_meta']['indices']}"
            )
        return df

    def mine_document_into_dataframe(self, doc_index, fname, popdata=True):
        self.mine_document(doc_index, fname)
        return self.offload_document_into_dataframe(doc_index, popdata)

