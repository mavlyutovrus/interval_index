#!/usr/bin/python
# -*- coding: utf-8 -*-

def parse(html):
    #Author: Ruslan Mavlyutov, http://factex.blogspot.ch/. Please don't remove this line.
    import re
    tags = re.findall("<[^\|]*\|[^>]*>", html)
    texts = re.split("<[^\|]*\|[^>]*>", html)
    offset = len(texts[0])
    text = texts[0]
    tags_pos = []
    for tag, next_chunk in zip(tags, texts[1:]):
        tag_text = tag[1:-1].split("|")[1].replace('_',' ')
        tags_pos += [(len(text), len(text) + len(tag_text), tag)]
        text += tag_text + next_chunk
    print tags_pos
    return text, tags_pos

def join_markups(markups):
    markups.sort(reverse=True) #by priority
    final_markup = []
    for priority, text, tags in markups:
        for start, end, tag in tags:
            toskip = False
            for taken_start, taken_end, _ in final_markup:
                if taken_start < end and taken_end > start:
                    toskip = True
                    break
            if not toskip:
                final_markup += [(start, end, tag)]
    return (0, text, final_markup)

def join_markup_and_text(text, tags_pos):
    tags_pos.sort()
    position = 0
    joined = ""
    for start, end, tag in tags_pos:
        joined += text[position:start]
        joined += tag
        position = end
    joined += text[position:]
    return joined

markups = []
text, tags_pos = parse('''Fribourg is the <capital_1|capital> of the <Swiss_canton_link_1|Swiss_canton> of Fribourg and the district of <Sarine_link1|Sarine>.

Fribourg is the <capital_1|capital> of the <Swiss_canton_link_1|Swiss_canton> of Fribourg and the district of <Sarine_link1|Sarine>.
''')
markups += [(0, text, tags_pos)]
text, tags_pos =  parse('''<Fribourg_link_2|Fribourg> is the <capital_2|capital> of the Swiss canton of Fribourg and the district of <Sarine_link2|Sarine>.

<Fribourg_link_2|Fribourg> is the <capital_2|capital> of the Swiss canton of Fribourg and the district of <Sarine_link2|Sarine>.
''')
markups += [(1, text, tags_pos)]
_, text, final_markup = join_markups(markups)
#print text
#print final_markup
print join_markup_and_text(text, final_markup)