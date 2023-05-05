# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 10:16:37 2023

Contains helper routines that are used throughout

@author: garya
"""

def sort_id(st):
    l = list(st)
    l.sort()
    return l


def xlate_to_str(inp,sep='; ',trunc=False,tlen=20,totallen = 5000,sort=True,
                maxlen=10000,maxMessage='Too many items to display'):
    """used to translate a list into a meaningful string for display"""
    try:
        if isinstance(inp,str):
            inp = [inp]
        l = list(inp)
        if sort:
            l.sort()
        if len(l)>maxlen:
            return maxMessage

        out = ''
        for a in l:
            s = str(a)
            if trunc:
                if len(s)>tlen:
                    s = s[:tlen-3]+ '...'
            out+= s+sep
        out = out[:-(len(sep))]
    except:
        return ''
    if len(out)>totallen:
        out = out[:totallen]+' ...' 
    return out


