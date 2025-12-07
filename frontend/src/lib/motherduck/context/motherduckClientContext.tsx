"use client"

import { fetchMotherDuckToken } from "../functions/fetchMotherDuckToken";
import initMotherDuckConnection from "../functions/initMotherDuckConnection";
import type { MaterializedQueryResult, MDConnection, SafeQueryResult } from "@motherduck/wasm-client";
import 'core-js/actual/promise/with-resolvers';
import { createContext, useContext, useEffect, useMemo, useRef } from "react";

// Type declaration for PromiseWithResolvers (ES2024 feature)
interface PromiseWithResolvers<T> {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: any) => void;
}

declare global {
  interface PromiseConstructor {
    withResolvers<T>(): PromiseWithResolvers<T>;
  }
}

// Safe interface for using the connection
interface MotherDuckContextValue {
  evaluateQuery: (query: string) => Promise<MaterializedQueryResult>;
  safeEvaluateQuery: (query: string) => Promise<SafeQueryResult<MaterializedQueryResult>>;

}

export const MotherDuckContext = createContext<MotherDuckContextValue | null>(null);

export function MotherDuckClientProvider({ children, database }: { children: React.ReactNode, database?: string },) {
  const connectionRef = useRef<PromiseWithResolvers<MDConnection | undefined> | undefined>(undefined);

  if (connectionRef.current === undefined) {
    connectionRef.current = Promise.withResolvers<MDConnection | undefined>();
  }

  const evaluateQuery = async (query: string): Promise<MaterializedQueryResult> => {
    if (!connectionRef.current) {
      throw new Error('MotherDuck connection ref is falsy')
    }

    const connection = await connectionRef.current.promise;

    if (!connection) {
      throw new Error('No MotherDuck connection available');
    }

    return connection.evaluateQuery(query);
  };

  const safeEvaluateQuery = async (query: string): Promise<SafeQueryResult<MaterializedQueryResult>> => {
    if (!connectionRef.current) {
      throw new Error('MotherDuck connection ref is falsy')
    }

    const connection = await connectionRef.current.promise;

    if (!connection) {
      throw new Error('No MotherDuck connection available');
    }

    return connection.safeEvaluateQuery(query);
  };

  useEffect(() => {
    const initializeConnection = async () => {
      try {
        const mdToken = await fetchMotherDuckToken();
        const result = initMotherDuckConnection(mdToken, database);
        if (connectionRef.current) {
          connectionRef.current.resolve(result);
        }

      } catch (error) {
        console.error(error);
      }
    };
    initializeConnection();

  }, []);

  const value = useMemo(() => ({
    evaluateQuery,
    safeEvaluateQuery,
  }), []);


  return (
    <MotherDuckContext.Provider value={value}>
      {children}
    </MotherDuckContext.Provider>
  );
}

export function useMotherDuckClientState() {
  const context = useContext(MotherDuckContext);
  if (!context) {
    throw new Error('useMotherDuckClientState must be used within MotherDuckClientStateProvider');
  }
  return context;
}