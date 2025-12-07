import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';


export async function GET(_request: NextRequest) {
    // The token provided to the frontend permits read-write access to data in the MotherDuck account.
    // Switch to MOTHERDUCK_READ_SCALING_TOKEN for a token with eventually consistent read-only access.
    // To generate short-lived tokens for use in the frontend, see the 
    // MotherDuck API documentation: https://api.motherduck.com/docs/
    return NextResponse.json({ mdToken: process.env.MOTHERDUCK_TOKEN || ''});
}