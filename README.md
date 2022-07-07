# Marvel

Create constraints to avoid duplicates and improve load performance:

    CREATE CONSTRAINT IF NOT EXISTS ON (c:Character) ASSERT (c.url) IS NODE KEY
    CREATE CONSTRAINT IF NOT EXISTS ON (g:Group) ASSERT (g.url) IS NODE KEY

Delete characters with unparsed names:

    MATCH(c:Character)
    WHERE c.name IS NULL
    DETACH DELETE c

These are probably just caused by a temporary crawling error. We will re-crawl to fill in these gaps.

    CREATE FULLTEXT INDEX IF NOT EXISTS FOR (c:Character) ON EACH [c.name]

Add PageRank:

    CALL gds.pageRank.write({
        nodeProjection: 'Character',
        relationshipProjection: 'REFERENCES',
        writeProperty: 'pageRank',
        maxIterations: 50
    })
    YIELD ranIterations, didConverge, createMillis, computeMillis, writeMillis, nodePropertiesWritten, centralityDistribution, configuration

Handy function to determine thresholds for Bloom sizing:

    MATCH(c:Character)
    RETURN percentileCont(c.pageRank, 0.95)


Add Louvain:

    CALL gds.louvain.write({
      nodeProjection: 'Character',
      relationshipProjection: 'REFERENCES',
      writeProperty: 'community'
    })
