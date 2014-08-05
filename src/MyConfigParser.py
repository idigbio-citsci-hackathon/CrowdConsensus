import collections
import os

LabelToken = "label"
DictToken = "dict"
TransToken = "trans"
FuncToken = "func"
KeyLabelToken = "key"

"""
    The object returned by the parser
"""
class ParsedConfig:
    def __init__(self):
        self.keyLabel = None
        self.labelMap = collections.defaultdict(LabelInfo)
        self.orderedLabels = []
        self.configFolder = ""

"""
    Information associated with a label
"""
class LabelInfo:
    def __init__(self):
        # Translation tables for this label
        self.trans = []
        # Dictionaries to be used for this label
        self.dicts = []
        # Functions to be used for this label
        self.funcs = []

"""
    Configuration file parsers. Returns ParsedConfig object representing the
    parsed configuration file
"""
def parseConfig(configFilePath):
    # The final object to return
    parsedConfig = ParsedConfig()
    parsedConfig.configFolder = os.path.dirname(configFilePath)

    # Maps labels to their attributes
    labelMap = collections.defaultdict(LabelInfo)
    # key label
    keyLabel = None

    currentLabels = []

    for line in open(configFilePath):
        # Ignore comments
        if line.startswith('#'):
            continue

        # Split the first token
        tokens = line.strip().split(': ')
        # The other tokens are delimited by ;
        temp = tokens[1].split(';')
        tokens = [tokens[0]]
        # Finalized tokens
        tokens.extend(temp)

        # Handle label token
        if (tokens[0] == LabelToken):
            # Keep track of the new label(s)
            currentLabels = []
            currentLabels.extend(tokens[1:])
            # Add to the label map, only if it isn't already there
            for label in currentLabels:
                if label not in parsedConfig.orderedLabels:
                    parsedConfig.orderedLabels.append(label)
        # Handle key label token
        elif (tokens[0] == KeyLabelToken):
            # cannot specify keylabel twice
            if (keyLabel):
                print "Key Label already specified as", keyLabel
                return None
            else:
                keys = []
                keys.extend(tokens[1:])
                if (len(keys) != 1):
                    print "Must specify ONE key label"
                else:
                    keyLabel = keys[0]
        # dict or trans
        else:
            if not currentLabels:
                print "Parse error: No labels specified for", tokens[1:]
                return None
            # Add each item to each of the current labels
            for item in tokens[1:]:
                for label in currentLabels:
                    if (tokens[0] == DictToken):
                        print "Adding dictionary", item, "to", label
                        labelMap[label].dicts.append(item)
                    elif (tokens[0] == TransToken):
                        print "Adding trans table", item, "to", label
                        labelMap[label].trans.append(item)
                    elif (tokens[0] == FuncToken):
                        print "Adding function", item, "to", label
                        labelMap[label].funcs.append(item)
                    else:
                        print "Unrecognized token: ", label[0]
    parsedConfig.labelMap = labelMap
    if (not keyLabel):
        print "No key label specified!"
        return None
    parsedConfig.keyLabel = keyLabel
    return parsedConfig
