package com.breezeehr.connect.clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.util.Map;
import java.util.NoSuchElementException;

public final class CljRequirer {
    public static IFn REQUIRING_RESOLVE = Clojure.var("clojure.core", "requiring-resolve");
    public static IFn SYMBOL = Clojure.var("clojure.core", "symbol");
    public static IFn KEYWORD = Clojure.var("clojure.core", "keyword");
    public static IFn DEREF = Clojure.var("clojure.core", "deref");
    static {
      String logns;
      if (System.getenv("PEDESTAL_LOGGER") != null){
        logns = System.getenv("PEDESTAL_LOGGER");
      }
      if (System.getProperty("io.pedestal.log.overrideLogger") != null){
        logns = System.getProperty("io.pedestal.log.overrideLogger");
      }
      if (logns != null){
        try {
          REQUIRING_RESOLVE.invoke(SYMBOL.invoke(logns));
        } catch(Throwable t) {
          throw new NoSuchElementException("Failed to load namespace '" + logns + "'" + t.getMessage());
        }
      }
    }
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
        Object required_var;
        if (varName == null) {
            throw new NoSuchElementException("Must provide reference to implementation at config key 'clj.impl'" );
        }
        try {
          required_var = REQUIRING_RESOLVE.invoke(SYMBOL.invoke(varName));
        } catch(Throwable t) {
            throw new NoSuchElementException("Failed to load namespace '" + varName + "'" + t.getMessage());
        }

        Object derefed_item = DEREF.invoke(required_var);
        if (derefed_item == null) {
            throw new NoSuchElementException("Var '" + varName + "' not found");
        }
        if (derefed_item instanceof Map) {
            return (Map) derefed_item;
        } else {
            throw new NoSuchElementException("value at clj.impl is not a map.");
        }
    }
}
