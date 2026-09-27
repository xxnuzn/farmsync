/*
저장소 어댑터 — 로컬 큐와 수신 오프셋을 브라우저에 남긴다.

세 가지 구현을 제공한다. sync-core 는 이 인터페이스만 알면 된다.
  - createMemoryStorage()   : Node 테스트용
  - createLocalStorage(ns)  : 단순·동기 방식 (기본)
  - createIndexedDBStorage(): 용량이 큰 환경용 (선택)
*/
(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    root.FarmSyncStorage = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  function createMemoryStorage() {
    var box = {};
    return {
      kind: "memory",
      get: function (k) { return Promise.resolve(box[k] === undefined ? null : box[k]); },
      set: function (k, v) { box[k] = v; return Promise.resolve(true); },
      remove: function (k) { delete box[k]; return Promise.resolve(true); },
    };
  }

  function createLocalStorage(namespace) {
    var ns = namespace || "farmsync";
    return {
      kind: "localStorage",
      get: function (k) {
        try {
          var raw = window.localStorage.getItem(ns + "::" + k);
          return Promise.resolve(raw ? JSON.parse(raw) : null);
        } catch (e) { return Promise.resolve(null); }
      },
      set: function (k, v) {
        try {
          window.localStorage.setItem(ns + "::" + k, JSON.stringify(v));
          return Promise.resolve(true);
        } catch (e) { return Promise.resolve(false); }
      },
      remove: function (k) {
        try { window.localStorage.removeItem(ns + "::" + k); } catch (e) {}
        return Promise.resolve(true);
      },
    };
  }

  function createIndexedDBStorage(dbName, storeName) {
    var dbp = null;
    function db() {
      if (dbp) return dbp;
      dbp = new Promise(function (resolve, reject) {
        var req = indexedDB.open(dbName || "farmsync", 1);
        req.onupgradeneeded = function () {
          var d = req.result;
          if (!d.objectStoreNames.contains(storeName || "kv")) d.createObjectStore(storeName || "kv");
        };
        req.onsuccess = function () { resolve(req.result); };
        req.onerror = function () { reject(req.error); };
      });
      return dbp;
    }
    function tx(mode, fn) {
      return db().then(function (d) {
        return new Promise(function (resolve, reject) {
          var t = d.transaction(storeName || "kv", mode);
          var s = t.objectStore(storeName || "kv");
          var r = fn(s);
          t.oncomplete = function () { resolve(r && r.result !== undefined ? r.result : true); };
          t.onerror = function () { reject(t.error); };
        });
      });
    }
    return {
      kind: "indexedDB",
      get: function (k) { return tx("readonly", function (s) { return s.get(k); }); },
      set: function (k, v) { return tx("readwrite", function (s) { return s.put(v, k); }); },
      remove: function (k) { return tx("readwrite", function (s) { return s.delete(k); }); },
    };
  }

  /** 브라우저 환경에 맞는 저장소를 자동 선택한다. */
  function autoStorage() {
    try {
      if (typeof window !== "undefined" && window.indexedDB) {
        return createIndexedDBStorage("farmsync", "kv");
      }
    } catch (e) { /* fallthrough */ }
    if (typeof window !== "undefined" && window.localStorage) return createLocalStorage("farmsync");
    return createMemoryStorage();
  }

  return {
    createMemoryStorage: createMemoryStorage,
    createLocalStorage: createLocalStorage,
    createIndexedDBStorage: createIndexedDBStorage,
    autoStorage: autoStorage,
  };
});
