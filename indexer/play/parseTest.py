import rfc822

rawAddr = "Antony Courtney <antony@antonycourtney.com>, Elizabeth Drew\r\n <elizadrew@gmail.com>, Christine Diehl <cldiehl@gmail.com>, Eirini\r\n Papastergiou <eirini.papastergiou@gmail.com>"
print "rawAddr: ", rawAddr
addrTuples = rfc822.AddressList(rawAddr).addresslist
print "--> addrTuples: ", addrTuples

# Let's use translate to strip out newline chars:
betterStr=rawAddr.translate(None, '\r\n')
print "\nbetterStr: ", betterStr
addrTuples = rfc822.AddressList(betterStr).addresslist
print "--> addrTuples: ", addrTuples
