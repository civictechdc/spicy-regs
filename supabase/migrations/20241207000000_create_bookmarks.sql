create table if not exists bookmarks (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users not null,
  resource_id text not null,
  resource_type text not null, -- 'dockey', 'document', 'comment'
  agency_code text not null,
  title text,
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz default now()
);

-- Enable RLS
alter table bookmarks enable row level security;

-- Policies
create policy "Users can view their own bookmarks" 
on bookmarks for select 
using (auth.uid() = user_id);

create policy "Users can insert their own bookmarks" 
on bookmarks for insert 
with check (auth.uid() = user_id);

create policy "Users can delete their own bookmarks" 
on bookmarks for delete 
using (auth.uid() = user_id);
