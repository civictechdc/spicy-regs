'use server'

import { createClient } from '@/utils/supabase/server'

import { redirect } from 'next/navigation'
import { revalidatePath } from 'next/cache'

export async function sendOtp(previousState: any, formData: FormData) {
  const supabase = await createClient()

  const email = formData.get('email') as string

  const { error } = await supabase.auth.signInWithOtp({
    email,
    options: {
      shouldCreateUser: false, 
    }
  })

  if (error) {
    return { error: error.message, success: false, message: '' }
  }

  return { success: true, message: 'Check your email for the code!', error: '' }
}

export async function verifyOtp(previousState: any, formData: FormData) {
  const supabase = await createClient()

  const email = formData.get('email') as string
  const token = formData.get('token') as string

  const { error } = await supabase.auth.verifyOtp({
    email,
    token,
    type: 'email',
  })

  if (error) {
    return { error: error.message, success: false }
  }

  revalidatePath('/dashboard', 'layout')
  redirect('/dashboard')
}
