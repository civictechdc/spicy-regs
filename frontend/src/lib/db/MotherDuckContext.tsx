"use client";

import React, { createContext, useContext, useEffect, useState, useCallback, ReactNode } from 'react';
import { MDConnection } from '@motherduck/wasm-client';

interface MotherDuckContextType {
  connection: MDConnection | null;
  isReady: boolean;
  isLoading: boolean;
  error: Error | null;
  runQuery: <T = any>(sql: string) => Promise<T[]>;
}

const MotherDuckContext = createContext<MotherDuckContextType | null>(null);

interface MotherDuckProviderProps {
  children: ReactNode;
}

export function MotherDuckProvider({ children }: MotherDuckProviderProps) {
  const [connection, setConnection] = useState<MDConnection | null>(null);
  const [isReady, setIsReady] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    async function initMotherDuck() {
      const token = process.env.NEXT_PUBLIC_MOTHERDUCK_TOKEN;
      
      if (!token) {
        setError(new Error("NEXT_PUBLIC_MOTHERDUCK_TOKEN is not set"));
        setIsLoading(false);
        return;
      }

      try {
        const conn = MDConnection.create({ mdToken: token });
        await conn.isInitialized();
        setConnection(conn);
        setIsReady(true);
        console.log("MotherDuck WASM initialized");
      } catch (err) {
        console.error("Failed to initialize MotherDuck:", err);
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
      }
    }

    // Only run in browser
    if (typeof window !== 'undefined') {
      initMotherDuck();
    }
  }, []);

  const runQuery = useCallback(async <T = any>(sql: string): Promise<T[]> => {
    if (!connection) {
      throw new Error("MotherDuck connection not initialized");
    }
    
    const result = await connection.evaluateQuery(sql);
    
    return result.data.toRows() as T[];
  }, [connection]);

  return (
    <MotherDuckContext.Provider value={{ connection, isReady, isLoading, error, runQuery }}>
      {children}
    </MotherDuckContext.Provider>
  );
}

export function useMotherDuck(): MotherDuckContextType {
  const context = useContext(MotherDuckContext);
  if (!context) {
    throw new Error("useMotherDuck must be used within a MotherDuckProvider");
  }
  return context;
}
