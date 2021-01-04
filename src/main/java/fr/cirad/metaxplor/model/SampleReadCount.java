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

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 *
 * @author petel, sempere
 */
@TypeAlias(SampleReadCount.TYPE_ALIAS)
public class SampleReadCount {
	public static final String TYPE_ALIAS = "SRC";
	
    public static final String FIELDNAME_SAMPLE_CODE = "sp";
    public static final String FIELDNAME_SAMPLE_COUNT = "n"; 

    @Field(FIELDNAME_SAMPLE_CODE)
    String sp;
    
    @Field(FIELDNAME_SAMPLE_COUNT)
    int count;
    
    public SampleReadCount() {
    }

    public SampleReadCount(String sampleCode, int count) {
        this.sp = sampleCode;
        this.count = count;
    }

	public String getSp() {
		return sp;
	}

	public void setSp(String sp) {
		this.sp = sp;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
