package com.breezeehr.connect.clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.util.Map;
import java.util.NoSuchElementException;

public final class CljRequirer {
    public static IFn REQUIRE = Clojure.var("clojure.core", "require");
    public static IFn SYMBOL = Clojure.var("clojure.core", "symbol");
    public static IFn KEYWORD = Clojure.var("clojure.core", "keyword");
    public static IFn DEREF = Clojure.var("clojure.core", "deref");

    static IFn getFN (Map m, String param) throws NoSuchElementException {
        Object item = m.get(KEYWORD.invoke(param));
        if (item instanceof IFn ){
            return (IFn) item;
        }
        return null;
    }
    static synchronized Map getVar(Map<String, String> config)
            throws NoSuchElementException {

        String varName = config.get("clj.impl");
        if (varName == null) {
            throw new NoSuchElementException("Must provide reference to implementation at config key 'clj.impl'" );
        }

        String[] parts = varName.split("/", 2);
        String namespace = parts[0];
        String name = parts[1];
        if (namespace == null || name == null) {
            throw new NoSuchElementException("Invalid namespace-qualified symbol '" + varName + "'");
        }

        try {
            REQUIRE.invoke(SYMBOL.invoke(namespace));
        } catch(Throwable t) {
            throw new NoSuchElementException("Failed to load namespace '" + namespace + "'" + t.getMessage());
        }

        Object item = DEREF.invoke(Clojure.var(namespace, name));
        if (item == null) {
            throw new NoSuchElementException("Var '" + varName + "' not found");
        }
        if (item instanceof Map) {
            return (Map) item;
        } else {
            throw new NoSuchElementException("value at clj.impl is not a map.");
        }
    }
}
