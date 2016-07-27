#coding=utf8
import sys,os,json
reload(sys)
sys.setdefaultencoding("utf8")
sys.path.append('./bin/')
import urllib2
import gzip
import StringIO
import socket
import copy
from bs4 import BeautifulSoup
from urlparse import urljoin
import re
socket.setdefaulttimeout(5)
from pysqlite2 import dbapi2 as sqlite
import jieba
from readability.readability import Document
import nn
mynet = nn.searchnet('nn.db')

class crawler:
    def __init__(self,dbname):
        self.data_home = os.getcwd()+"/data/"

        self.db_path = dbname
        #if os.path.exists(self.db_path):
        #    os.remove(self.db_path)
        self.con = sqlite.connect(self.db_path)
        
        self.doclib = self.data_home + "/doclib/"
        if not os.path.exists(self.doclib):
            os.mkdir(self.doclib)
        self.stop_words = ['\n']

    def __del__(self):
        self.con.commit()
        self.con.close()
    def dbcommit(self):
        self.con.commit()
    def getentryid(self,table,field,value,createnew=True):
        cur = self.con.execute( 
            "select rowid from %s where %s='%s'" % (table,field,value)
        )
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                "insert into %s (%s) values ('%s')" % (table,field,value )
            )
            return cur.lastrowid
        else:
            return res[0]
    def addtoindex(self,url,soup,page):
        if self.isindexed(url):
            return
        print 'Indexing %s' % url
        pure_text = ""
        try:
            doc = Document(page)
            cent = doc.summary()
            csoup = BeautifulSoup(cent)
            pure_text = self.gettextonly(csoup)
            
            pat = re.compile("<[^>]+>",re.S)
            pure_text = pat.sub("",pure_text) 
            pat = re.compile("\n+",re.S)
            pure_text = pat.sub("\n",pure_text) 
        except Exception,e:
            print e
            return
                
        title = doc.title()
        title = re.search("([^|_]*).*",title).groups()[0]
        
        fname = "%s/%s.txt" % (self.doclib,title)
        fh = open(fname,"wr")
        fh.write("%s\n%s\n%s\n" %(title,url,pure_text) )
        
        words = self.separatewords(pure_text)
        #print " ".join(words)
        urlid = self.getentryid('urllist','url',url)
        for i in range(len(words)):
            word = words[i]
            if word in self.stop_words:
                continue
            wordid = self.getentryid('wordlist','word',word)
            sql = 'insert into wordlocation(urlid,wordid,location) \
                values (%d,%d,%d )' % (urlid,wordid,i )
            print sql
            self.con.execute(sql)

    def gettextonly(self,soup):
        v = soup.string
        if v == None:
            cs = soup.contents
            rtext = ""
            for t in cs:
                subtext = self.gettextonly(t)
                rtext += subtext + "\n"
            return rtext
        else:
            return v.strip()        
    def separatewords(self,text):
        words = [w.encode("utf8") for w in jieba.cut(text) ]
        return words
    def isindexed(self,url):
        u = self.con.execute(
            "select rowid from urllist where url = '%s'" % url
           ).fetchone()
        if u != None:
            v = self.con.execute(
                "select * from wordlocation where urlid = %d" % u[0]
            ).fetchone()
            if v != None:
                return True
        return False
    def addlinkref(self,urlFrom,urlTo,linkText):
        pass
    def crawl(self,pages,depth=2):
        cur_depth = 0
        print pages
        cats = ['sports.sina.com.cn','mil.news.sina.com.cn','finance.sina.com.cn','ent.sina.com.cn','tech.sina.com.cn']
        for i in range(depth):
            newpages =set()
            print "depth:%d" % (i)
            for page in pages:
                pc = ""
                try:
                    c = urllib2.urlopen(page).read()
                    pc = gzip.GzipFile(fileobj=StringIO.StringIO(c)).read()
                except IOError,e:
                    pc = c
                except Exception,e:
                    print "could not open" + url
                    continue

                soup = BeautifulSoup(pc)
                #print pc[0:20]
                #open("index.txt",'w').write(pc)
                if page.endswith("tml"):
                    self.addtoindex(page,soup,pc)
                    #return
                links= soup('a')
                #print links
                max_width = 5
                link_num = 0
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page,link['href'])
                        if url.find("'") != -1 :continue
                        url = url.split("#")[0]
                        if url[0:4] != 'http' or url[-3:]!='tml' or self.isindexed(url):
                            continue
                        hostg = re.search("http://([^/]*).*", url)
                        if hostg != None and  hostg.groups()[0] in cats:
                            newpages.add(url)
                            linkText = self.gettextonly(link)
                            self.addlinkref(page,url,linkText)
                            print url
                            link_num += 1
                            if link_num >=5:
                                break;
                self.dbcommit()
            #print newpages
            pages = newpages    
    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()
    def calculatepagerank(self,iterations=20):
        self.con.execute("drop table if exists pagerank")
        self.con.execute("create table pagerank(urlid primary key,score)")

        self.con.execute('insert into pagerank select rowid,1.0 from urllist')
        self.dbcommit()
        for i in range(iterations):
            print "Iteration %d" % i
            for (urlid,) in self.con.execute('select rowid from urllist'):
                pr = 0.15
                for (linker,) in self.con.execute(
                    "select distinct fromid from link where toid=%d" % urlid):
                    linkingpr = self.con.execute(
                    "select score from pagerank where urlid=%d" % linker).fetchone()[0]

                    linkingcount=self.con.execute(
                    "select count(*) from link where fromid=%d" % linker).fetchone()[0]
                    pr += 0.85 * (linkingpr/linkingcount)
                self.con.execute(
                'update pagerank set score = %f where urlid=%d' % (pr,urlid))
            self.dbcommit()         

class searcher():
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)
    def __del__ (self):
        self.con.commit()
        self.con.close()
    def getmatchrows(self,q):
        fieldlist = "w0.urlid"
        tablelist = ""
        clauselist = ''
        wordids=[]
        
        words = q.split(' ')
        tablenumber=0
        
        for word in words:
            wordrow = self.con.execute(
                "select rowid from wordlist where word = '%s'" % word
            ).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber>0:
                    tablelist+=','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid = w%d.urlid and ' %(tablenumber-1,tablenumber )
                fieldlist+=',w%d.location ' % tablenumber
                tablelist+= 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber,wordid)
                tablenumber += 1
        fullquery = 'select %s from %s where %s' % (fieldlist,tablelist,clauselist) 
        #print fullquery
        cur = self.con.execute(fullquery)
        rows = [row for row in cur ]
        #print wordids,rows[0],len(rows)
        return rows,wordids
    def getscoredlist(self,rows,wordids):
        totalscores = dict([(row[0],0 ) for row in rows])   
        weights = [(1,self.frequencyscore(rows))
            ,(1,self.locationscore(rows))
            ,(1,self.distancescore(rows))
            ,(1.0,self.pagerankscore(rows))
            ,(1.0,self.linktextscore(rows,wordids))
            ]
        for (weight,scores) in weights:
            for url in totalscores:
                totalscores[url] += weight*scores[url] 
        return totalscores
    def geturlname(self,id):
        return self.con.execute(
            "select url from urllist where rowid = %d" % id
            ).fetchone()[0]
    def query(self,q):
        rows,wordids = self.getmatchrows(q)
        scores=self.getscoredlist(rows,wordids)
        rankedscores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
        for (score,urlid) in rankedscores[0:10] :
            print '%.6f\t%s' % (score,self.geturlname(urlid))
    def normalizescores(self,scores,smallIsBetter=0):
        vsmall = 0.00001
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l))for (u,l) in scores.items() ]   )
        else:
            maxscore = max(scores.values())
            if maxscore == 0:
                maxscore = vsmall
            return dict([ (u,float(c)/maxscore) for (u,c) in scores.items()  ]  )               
    def frequencyscore(self,rows):
        counts = dict([(row[0],0) for row in rows])
        for row in rows:counts[row[0]] += 1
        return self.normalizescores(counts)
    def locationscore(self,rows):
        locations = dict([row[0],10000] for row in rows )
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:locations[row[0]] = loc
        return self.normalizescores(locations,smallIsBetter=1)
    def distancescore(self,rows):
        if len(rows) <= 2: return dict([(row[0],1.0) for row in rows])
        mindistance = dict([(row[0],100000)for row in rows])
        for row in rows:
            dist =sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
            if dist< mindistance[row[0]]:mindistance[row[0]]=dist
        return self.normalizescores(mindistance,smallIsBetter=1)
    def inboundlinkscore(self,rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict(
            [(u,self.con.execute(
                "select count(*) from link where toid='%d'" % u).fetchone()[0]) for u in uniqueurls])
        return self.normalizescores(inboundcount)
    def pagerankscore(self,rows):
        pageranks=dict([ (row[0],self.con.execute(
            "select score from pagerank where urlid=%d" % row[0]).fetchone()[0]) for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores=dict([(u,float(l)/maxrank)for (u,l) in pageranks.items()])
        return normalizedscores
        
    def linktextscore(self,rows,wordids):
        linkscores=dict([(row[0],0) for row in rows])
        for wordid in wordids:
            cur=self.con.execute(
            "select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid" % wordid)
            for (fromid,toid) in cur:
                if toid in linkscores:
                    pr=self.con.execute("select score from pagerank where urlid=%d" % toid).fetchone()[0]
                    linkscores[toid]+=pr
        return self.normalizescores(linkscores)

    def nnscore(self,rows,wordids):
        urlids=[urlid for urlid in set([row[0] for row in rows])]
        nnres = mynet.getresult(wordids,urlids)
        scores = dict([ (urlids[i],nnres[i]) for i in range(len(urlids))])
        return self.normalizescores(scores)                          
if __name__ == '__main__':
    print "hi"
    cra = crawler('data/searchindex.db')
    pages = ["http://sports.sina.com.cn/","http://mil.news.sina.com.cn",'http://finance.sina.com.cn/','http://ent.sina.com.cn/','http://tech.sina.com.cn/']
    #cra.createindextables()
    #cra.crawl(pages)
    #cra.calculatepagerank()
    ser = searcher('data/searchindex.db')
    ser.query('中国 世界')

