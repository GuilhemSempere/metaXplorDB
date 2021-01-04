/*******************************************************************************
 * metaXplorDB - Copyright (C) 2020 <CIRAD>
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.metaxplor.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * map collection 'counter' to keep records of auto-incremented fields
 *
 * @author petel, sempere
 */
@Document(collection = "counters")
@TypeAlias("AIC")
public class AutoIncrementCounter {

    @Id
    private final String id;
    private int seq;

    public AutoIncrementCounter(String id, int seq) {
        super();
        this.id = id;
        this.seq = seq;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    /**
     *
     * @param mongo
     * @param collectionName
     * @return
     */
    public static synchronized int getNextSequence(MongoTemplate mongoTemplate, String collectionName) {
        AutoIncrementCounter counter = mongoTemplate.findAndModify(new Query(Criteria.where("_id").is(collectionName)), new Update().inc("seq", 1), FindAndModifyOptions.options().returnNew(true), AutoIncrementCounter.class);
        if (counter != null)
            return counter.getSeq();

        // counters collection contains no data for this type
        counter = new AutoIncrementCounter(collectionName, 1);
        mongoTemplate.save(counter);
        return 1;
    }
    
    public static synchronized void ensureCounterIsAbove(MongoTemplate mongoTemplate, String collectionName, int minVal) {
    	AutoIncrementCounter counter = mongoTemplate.findById(collectionName, AutoIncrementCounter.class);
    	if (counter == null) {
            counter = new AutoIncrementCounter(collectionName, minVal);
            mongoTemplate.save(counter);
    	}

    	if (counter.getSeq() < minVal)
    		mongoTemplate.updateFirst(new Query(Criteria.where("_id").is(collectionName)), new Update().set("seq", minVal), AutoIncrementCounter.class);
    }
}
