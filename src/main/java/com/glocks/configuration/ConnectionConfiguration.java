package com.glocks.configuration;

import java.sql.Connection;
import java.sql.SQLException;


import org.springframework.stereotype.Repository;
//

import org.springframework.orm.jpa.EntityManagerFactoryInfo;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Repository
public class ConnectionConfiguration {
	
	@PersistenceContext
    private EntityManager em;
	
	public Connection getConnection() {
		EntityManagerFactoryInfo info = (EntityManagerFactoryInfo) em.getEntityManagerFactory();
	    try {
			return info.getDataSource().getConnection();
		} catch (SQLException e) {
			return null;
		}
	}
	
}
