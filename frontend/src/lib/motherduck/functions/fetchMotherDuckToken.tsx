// This function fetches a MotherDuck token for the frontend to use to connect to MotherDuck.
export async function fetchMotherDuckToken(): Promise<string> {
    const response = await fetch(`/api/md-token`)
    const { mdToken } = await response.json()
    return mdToken
}