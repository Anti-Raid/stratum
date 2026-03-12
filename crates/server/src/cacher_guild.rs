//! Constructs a guild object from cache if possible. Based on https://github.com/Gelbpunkt/gateway-proxy/blob/main/src/cache.rs

use twilight_model::{channel::{Channel, StageInstance, message::Sticker}, gateway::presence::{Presence, UserOrId}, guild::{Emoji, Guild, Member, Role, scheduled_event::GuildScheduledEvent}, id::{Id, marker::{GuildMarker, UserMarker}}, voice::VoiceState};
use rayon::prelude::*;
use stratum_common::{GuildFetchOpts, pb};

const MIN_CHANNELS_TILL_THREADPOOL: usize = 100;
const MIN_MEMBERS_TILL_THREADPOOL: usize = 1000;
const MIN_PRESENCES_TILL_THREADPOOL: usize = 1000;

fn channels_and_threads(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> (Vec<Channel>, Vec<Channel>) {
    cache.guild_channels(guild_id).map(|reference| {
        if reference.len() < MIN_CHANNELS_TILL_THREADPOOL {
            reference.iter().filter_map(|id| {
                let chan = cache.channel(*id)?;
                Some(chan.value().clone())
            }).partition(|chan| !chan.kind.is_thread())
        } else {
            reference.par_iter().filter_map(|id| {
                let chan = cache.channel(*id)?;
                Some(chan.value().clone())
            }).partition(|chan| !chan.kind.is_thread())
        }
    }).unwrap_or_default()
}

fn presences_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<Presence> {
    cache
        .guild_presences(guild_id)
        .map(|reference| {
            if reference.len() < MIN_PRESENCES_TILL_THREADPOOL {
                reference
                    .iter()
                    .filter_map(|user_id| {
                        let presence = cache.presence(guild_id, *user_id)?;

                        Some(Presence {
                            activities: presence.activities().to_vec(),
                            client_status: presence.client_status().clone(),
                            guild_id: presence.guild_id(),
                            status: presence.status(),
                            user: UserOrId::UserId {
                                id: presence.user_id(),
                            },
                        })
                    })
                    .collect()
            } else {
                reference
                    .par_iter()
                    .filter_map(|user_id| {
                        let presence = cache.presence(guild_id, *user_id)?;

                        Some(Presence {
                            activities: presence.activities().to_vec(),
                            client_status: presence.client_status().clone(),
                            guild_id: presence.guild_id(),
                            status: presence.status(),
                            user: UserOrId::UserId {
                                id: presence.user_id(),
                            },
                        })
                    })
                    .collect()
            }
        })
        .unwrap_or_default()
}

fn emojis_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<Emoji> {
    cache
        .guild_emojis(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|emoji_id| {
                    let emoji = cache.emoji(*emoji_id)?;

                    Some(Emoji {
                        animated: emoji.animated(),
                        available: emoji.available(),
                        id: emoji.id(),
                        managed: emoji.managed(),
                        name: emoji.name().to_string(),
                        require_colons: emoji.require_colons(),
                        roles: emoji.roles().to_vec(),
                        user: emoji
                            .user_id()
                            .and_then(|id| cache.user(id).map(|user| user.value().clone())),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Given cache, guild_id and user_id, returns a member
pub fn member(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>, user_id: Id<UserMarker>) -> Option<Member> {
    let member = cache.member(guild_id, user_id)?;

    Some(Member {
        avatar: member.avatar(),
        communication_disabled_until: member.communication_disabled_until(),
        deaf: member.deaf().unwrap_or_default(),
        flags: member.flags(),
        joined_at: member.joined_at(),
        mute: member.mute().unwrap_or_default(),
        nick: member.nick().map(ToString::to_string),
        pending: member.pending(),
        premium_since: member.premium_since(),
        roles: member.roles().to_vec(),
        user: cache.user(member.user_id())?.value().clone(),

        // Not yet exposed by Twilight
        avatar_decoration_data: None,
        banner: None,
    })
}

fn members_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<Member> {
    cache
        .guild_members(guild_id)
        .map(|reference| {
            if reference.len() < MIN_MEMBERS_TILL_THREADPOOL {
                reference
                    .iter()
                    .filter_map(|user_id| member(cache, guild_id, *user_id))
                    .collect()
            } else {
                reference
                    .par_iter()
                    .filter_map(|user_id| member(cache, guild_id, *user_id))
                    .collect()
            }
        })
        .unwrap_or_default()
}

fn roles_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<Role> {
    cache
        .guild_roles(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|role_id| Some(cache.role(*role_id)?.value().resource().clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn scheduled_events_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<GuildScheduledEvent> {
    cache
        .scheduled_events(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|event_id| {
                    Some(
                        cache
                            .scheduled_event(*event_id)?
                            .value()
                            .resource()
                            .clone(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn stage_instances_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<StageInstance> {
    cache
        .guild_stage_instances(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|stage_id| {
                    Some(cache.stage_instance(*stage_id)?.value().resource().clone())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn stickers_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<Sticker> {
    cache
        .guild_stickers(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|sticker_id| {
                    let sticker = cache.sticker(*sticker_id)?;

                    Some(Sticker {
                        available: sticker.available(),
                        description: Some(sticker.description().to_string()),
                        format_type: sticker.format_type(),
                        guild_id: Some(sticker.guild_id()),
                        id: sticker.id(),
                        kind: sticker.kind(),
                        name: sticker.name().to_string(),
                        pack_id: sticker.pack_id(),
                        sort_value: sticker.sort_value(),
                        tags: sticker.tags().to_string(),
                        user: sticker
                            .user_id()
                            .and_then(|id| cache.user(id).map(|user| user.value().clone())),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn voice_states_in_guild(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Vec<VoiceState> {
    cache
        .guild_voice_states(guild_id)
        .map(|reference| {
            reference
                .iter()
                .filter_map(|user_id| {
                    let voice_state = cache.voice_state(*user_id, guild_id)?;

                    Some(VoiceState {
                        channel_id: Some(voice_state.channel_id()),
                        deaf: voice_state.deaf(),
                        guild_id: Some(voice_state.guild_id()),
                        member: member(cache, guild_id, *user_id),
                        mute: voice_state.mute(),
                        self_deaf: voice_state.self_deaf(),
                        self_mute: voice_state.self_mute(),
                        self_stream: voice_state.self_stream(),
                        self_video: voice_state.self_video(),
                        session_id: voice_state.session_id().to_string(),
                        suppress: voice_state.suppress(),
                        user_id: voice_state.user_id(),
                        request_to_speak_timestamp: voice_state.request_to_speak_timestamp(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Gets a guild object
pub fn get_guild(
    cache: &twilight_cache_inmemory::InMemoryCache,
    guild_id: Id<GuildMarker>,
    flags: GuildFetchOpts
) -> Option<Guild> {
    let Some(guild) = cache.guild(guild_id) else {
        return None;
    };

    let (guild_channels, threads) = channels_and_threads(cache, guild.id());
    let presences = if flags.contains(GuildFetchOpts::INCLUDE_PRESENCES) { presences_in_guild(cache, guild.id()) } else { vec![] };
    let emojis = emojis_in_guild(cache, guild.id());
    let members = if flags.contains(GuildFetchOpts::INCLUDE_MEMBERS) { members_in_guild(cache, guild.id()) } else { vec![] };
    let roles = roles_in_guild(cache, guild.id());
    let scheduled_events = scheduled_events_in_guild(cache, guild.id());
    let stage_instances = stage_instances_in_guild(cache, guild.id());
    let stickers = stickers_in_guild(cache, guild.id());
    let voice_states = voice_states_in_guild(cache, guild.id());

    let new_guild = Guild {
        afk_channel_id: guild.afk_channel_id(),
        afk_timeout: guild.afk_timeout(),
        application_id: guild.application_id(),
        approximate_member_count: None, // Only present in with_counts HTTP endpoint
        banner: guild.banner().map(ToOwned::to_owned),
        approximate_presence_count: None, // Only present in with_counts HTTP endpoint
        channels: guild_channels,
        default_message_notifications: guild.default_message_notifications(),
        description: guild.description().map(ToString::to_string),
        discovery_splash: guild.discovery_splash().map(ToOwned::to_owned),
        emojis,
        explicit_content_filter: guild.explicit_content_filter(),
        features: guild.features().cloned().collect(),
        guild_scheduled_events: scheduled_events,
        icon: guild.icon().map(ToOwned::to_owned),
        id: guild.id(),
        joined_at: guild.joined_at(),
        large: guild.large(),
        max_members: guild.max_members(),
        max_presences: guild.max_presences(),
        max_stage_video_channel_users: guild.max_stage_video_channel_users(),
        max_video_channel_users: guild.max_video_channel_users(),
        member_count: guild.member_count(),
        members,
        mfa_level: guild.mfa_level(),
        name: guild.name().to_string(),
        nsfw_level: guild.nsfw_level(),
        owner_id: guild.owner_id(),
        owner: guild.owner(),
        permissions: guild.permissions(),
        public_updates_channel_id: guild.public_updates_channel_id(),
        preferred_locale: guild.preferred_locale().to_string(),
        premium_progress_bar_enabled: guild.premium_progress_bar_enabled(),
        premium_subscription_count: guild.premium_subscription_count(),
        premium_tier: guild.premium_tier(),
        presences,
        roles,
        rules_channel_id: guild.rules_channel_id(),
        safety_alerts_channel_id: guild.safety_alerts_channel_id(),
        splash: guild.splash().map(ToOwned::to_owned),
        stage_instances,
        stickers,
        system_channel_flags: guild.system_channel_flags(),
        system_channel_id: guild.system_channel_id(),
        threads,
        unavailable: Some(false),
        vanity_url_code: guild.vanity_url_code().map(ToString::to_string),
        verification_level: guild.verification_level(),
        voice_states,
        widget_channel_id: guild.widget_channel_id(),
        widget_enabled: guild.widget_enabled(),
    };

    Some(new_guild)
}

/// Returns the roles of a guild, unlike `roles_in_guild`, this directly makes a pb::AnyValue for efficiency reasons
/// and avoids cloning every role
pub fn get_roles_resource(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Result<pb::AnyValue, tonic::Status> {
    let Some(roles) = cache
        .guild_roles(guild_id) else {
            return pb::AnyValue::from_real(&None::<Vec<Role>>);
        };

    let roles: Vec<_> = roles
        .iter()
        .filter_map(|id| cache.role(*id))
        .collect(); // roles now holds the Reference guards

    let role_refs: Vec<&Role> = roles
        .iter()
        .map(|r| r.value().resource())
        .collect(); 
    
    pb::AnyValue::from_real(&role_refs)
}

/// Returns the channels of a guild, Unlike `channels_and_threads`, this directly makes a pb::AnyValue for efficiency reasons
/// and avoids cloning every channel
pub fn get_channels_resource(cache: &twilight_cache_inmemory::InMemoryCache, guild_id: Id<GuildMarker>) -> Result<pb::AnyValue, tonic::Status> {
    let Some(chans) = cache
        .guild_channels(guild_id) else {
            return pb::AnyValue::from_real(&None::<Vec<Channel>>);
        };

    let chans: Vec<_> = chans
        .iter()
        .filter_map(|id| cache.channel(*id))
        .collect(); // roles now holds the Reference guards

    let chan_refs: Vec<&Channel> = chans
        .iter()
        .map(|r| r.value())
        .collect(); 
    
    pb::AnyValue::from_real(&chan_refs)
}
