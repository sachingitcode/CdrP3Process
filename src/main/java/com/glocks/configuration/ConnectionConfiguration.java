package com.glocks.configuration;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.SQLException;


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
