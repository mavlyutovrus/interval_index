
all = []
prev_chr_id = ""
offset = 0
for line in open("all_chipseq.txt"):
    chr_id, first, last = line.split(" ")
    first = int(first) + offset
    last = int(last) + offset
    if chr_id != prev_chr_id:
        if chunk:
            all += [chunk]
        chunk = []
        prev_chr_id = chr_id