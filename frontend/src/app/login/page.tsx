'use client'

import { useState } from 'react'
import { sendOtp, verifyOtp } from './actions'

const initialState = {
  error: '',
  success: false,
  message: '',
}

export default function LoginPage() {
  const [emailSent, setEmailSent] = useState(false)
  const [email, setEmail] = useState('')
  const [state, setState] = useState(initialState)
  const [isPending, setIsPending] = useState(false)

  async function handleSendOtp(formData: FormData) {
    setIsPending(true)
    const result = await sendOtp(initialState, formData)
    setIsPending(false)

    if (result.success) {
      setEmailSent(true)
      setEmail(formData.get('email') as string)
      setState({ error: '', success: true, message: result.message || '' })
    } else {
      setState({ error: result.error || 'An error occurred', success: false, message: '' })
    }
  }

  async function handleVerifyOtp(formData: FormData) {
    setIsPending(true)
    // append email to formData since the verify action needs it
    formData.append('email', email)
    const result = await verifyOtp(initialState, formData)
    setIsPending(false)
    
    if (result && result.error) {
       setState({ error: result.error, success: false, message: '' })
    }
  }

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-gray-900 py-12 sm:px-6 lg:px-8">
      <div className="sm:mx-auto sm:w-full sm:max-w-md">
        <h2 className="mt-6 text-center text-3xl font-extrabold text-white">
          Sign in to your account
        </h2>
      </div>

      <div className="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
        <div className="bg-gray-800 py-8 px-4 shadow sm:rounded-lg sm:px-10 border border-gray-700">
            {state?.error && (
              <div className="mb-4 rounded-md bg-red-900/50 p-4 border border-red-800">
                <div className="flex">
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-red-200">
                      Error with your submission
                    </h3>
                    <div className="mt-2 text-sm text-red-300">
                      <p>{state.error}</p>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {state?.success && (
              <div className="mb-4 rounded-md bg-green-900/50 p-4 border border-green-800">
                <div className="flex">
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-green-200">
                      Success
                    </h3>
                    <div className="mt-2 text-sm text-green-300">
                      <p>{state.message}</p>
                    </div>
                  </div>
                </div>
              </div>
            )}
            
            {!emailSent ? (
              <form action={handleSendOtp} className="space-y-6">
                <div>
                  <label
                    htmlFor="email"
                    className="block text-sm font-medium text-gray-300"
                  >
                    Email address
                  </label>
                  <div className="mt-1">
                    <input
                      id="email"
                      name="email"
                      type="email"
                      autoComplete="email"
                      required
                      className="appearance-none block w-full px-3 py-2 border border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-gray-700 text-white"
                    />
                  </div>
                </div>

                <div>
                  <button
                    type="submit"
                    disabled={isPending}
                    className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 transition-colors"
                  >
                    {isPending ? 'Sending Code...' : 'Send Code'}
                  </button>
                </div>
              </form>
            ) : (
              <form action={handleVerifyOtp} className="space-y-6">
                 <div>
                  <label
                    htmlFor="token"
                    className="block text-sm font-medium text-gray-300"
                  >
                    Enter OTP Code
                  </label>
                  <div className="mt-1">
                    <input
                      id="token"
                      name="token"
                      type="text"
                      inputMode="numeric"
                      pattern="[0-9]*"
                      autoComplete="one-time-code"
                      required
                      className="appearance-none block w-full px-3 py-2 border border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-gray-700 text-white"
                    />
                  </div>
                </div>

                <div>
                  <button
                    type="submit"
                    disabled={isPending}
                    className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 transition-colors"
                  >
                    {isPending ? 'Verifying...' : 'Verify Code'}
                  </button>
                </div>
                 <div className="text-center">
                    <button 
                        type="button" 
                        onClick={() => setEmailSent(false)}
                        className="text-sm text-indigo-400 hover:text-indigo-300"
                    >
                        Use a different email
                    </button>
                </div>
              </form>
            )}
        </div>
      </div>
    </div>
  )
}
