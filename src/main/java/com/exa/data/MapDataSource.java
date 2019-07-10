package com.exa.data;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.sql.XASQLDataSource;
import com.exa.data.web.WSDataSource;

public class MapDataSource implements XADataSource {
	
	protected MapGetter mapGetter;

	public MapDataSource(MapGetter mapGetter) {
		super();
		this.mapGetter = mapGetter;
	}

	@Override
	public XASQLDataSource asXASQLDataSource() {
		return null;
	}

	@Override
	public WSDataSource asXAWSDataSource() {
		return null;
	}

	public MapGetter getMapGetter() {
		return mapGetter;
	}

	public void setMapGetter(MapGetter mapGetter) {
		this.mapGetter = mapGetter;
	}

	@Override
	public MapDataSource asMapDataSource() {
		return this;
	}
	

}
