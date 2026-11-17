import 'dotenv/config';
import { Client, Collection, GatewayIntentBits } from 'discord.js';
import { REST } from '@discordjs/rest';
import express from 'express';
import cron from 'node-cron';

import config from './config/application.js';
import { initializeDatabase } from './utils/database.js';
import { getGuildConfig } from './services/config/guildConfig.js';
import { getWelcomeConfig, updateWelcomeConfig } from './utils/database.js';
import { getServerCounters, saveServerCounters, updateCounter } from './services/serverstatsService.js';
import { logger, startupLog, shutdownLog } from './utils/logger.js';
import { checkBirthdays } from './services/birthdayService.js';
import { checkGiveaways } from './services/giveawayService.js';
import { loadCommands, registerCommands as registerSlashCommands } from './handlers/loaders/commandLoader.js';
import { runSafeTask, handleTaskError, ErrorCodes } from './utils/errorHandler.js';
import { initializeMusic } from './services/music/riffySetup.js';
import { shutdownMusic } from './services/music/playerHandler.js';
import pkg from '../package.json' with { type: 'json' };
import { EXPECTED_SCHEMA_VERSION, EXPECTED_SCHEMA_LABEL } from './config/database/schemaVersion.js';

class TitanBot extends Client {
  constructor() {
    super({
      intents: [
        
        GatewayIntentBits.Guilds,                        
        GatewayIntentBits.GuildMembers,                 

        GatewayIntentBits.GuildMessages,                
        GatewayIntentBits.GuildMessageReactions,        
        GatewayIntentBits.MessageContent,               
        GatewayIntentBits.DirectMessages,

        GatewayIntentBits.GuildVoiceStates,             

        GatewayIntentBits.GuildBans,                    
      ],
    });

    this.config = config;
    this.commands = new Collection();
    this.events = new Collection();
    this.buttons = new Collection();
    this.selectMenus = new Collection();
    this.modals = new Collection();
    this.cooldowns = new Collection();
    this.db = null;
    this.rest = new REST({ version: '10' }).setToken(config.bot.token);
  }

  async start() {
    try {
      startupLog('Starting TitanBot...');
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      startupLog('Initializing database...');
      const dbInstance = await initializeDatabase();
      this.db = dbInstance.db;

      // Check database status and report
      const dbStatus = this.db.getStatus();
      if (dbStatus.isDegraded) {
        logger.warn('');
        logger.warn('╔═══════════════════════════════════════════════════════╗');
        logger.warn('║ ⚠️  DATABASE RUNNING IN DEGRADED MODE                 ║');
        logger.warn('║                                                       ║');
        logger.warn('║ Connection: In-Memory Storage (MongoDB unavailable)║');
        logger.warn('║ Data Persistence: DISABLED - data lost on restart    ║');
        logger.warn('║ Action Required: Fix MongoDB and restart bot      ║');
        logger.warn('╚═══════════════════════════════════════════════════════╝');
        logger.warn('');
      } else {
        startupLog(`✅ Database Status: ${dbStatus.connectionType} (fully operational)`);
      }
      
      startupLog('Starting web server...');
      this.startWebServer();
      
      startupLog('Loading commands...');
      await loadCommands(this);
      startupLog(`Commands loaded: ${this.commands.size}`);
      
      startupLog('Loading handlers...');
      await this.loadHandlers();
      startupLog('Handlers loaded');

      initializeMusic(this);
      
      startupLog('Logging into Discord...');
      await this.login(this.config.bot.token);
      startupLog('Discord login successful');
      
      startupLog('Registering slash commands globally...');
      await this.registerCommands();
      startupLog('Slash commands registration complete');
      
      const databaseMode = dbStatus.isDegraded
        ? 'Optional in-memory mode (data resets after restart)'
        : 'Connected (persistent data enabled)';
      const handlerSummary = `${this.buttons.size} buttons, ${this.selectMenus.size} menus, ${this.modals.size} modals`;
      startupLog(
        `ONLINE ✅ | ${this.commands.size} commands loaded | ${handlerSummary} | Database: ${databaseMode}`
      );
      
      this.setupCronJobs();
    } catch (error) {
      logger.error('Failed to start bot:', error);
      process.exit(1);
    }
  }

  startWebServer() {
    const app = express();
    const configuredPort = Number(this.config.api?.port || process.env.PORT || 3000);
    const maxPortRetryAttempts = Number(process.env.PORT_RETRY_ATTEMPTS || 5);
    const host = process.env.WEB_HOST || '0.0.0.0';
    const corsOrigin = this.config.api?.cors?.origin || '*';
    
    app.use((req, res, next) => {
      const allowedOrigins = Array.isArray(corsOrigin) ? corsOrigin : [corsOrigin];
      const origin = req.headers.origin;
      
      if (allowedOrigins.includes('*') || allowedOrigins.includes(origin)) {
        res.header('Access-Control-Allow-Origin', origin || '*');
      }
      res.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      
      if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
      }
      next();
    });

    app.use(express.json({ limit: '64kb' }));

    const requestCounts = new Map();
    const windowMs = this.config.api?.rateLimit?.windowMs || 60000;
    const maxRequests = this.config.api?.rateLimit?.max || 100;
    
    app.use((req, res, next) => {
      const ip = req.ip;
      const now = Date.now();
      const windowStart = now - windowMs;
      
      if (!requestCounts.has(ip)) {
        requestCounts.set(ip, []);
      }
      
      const times = requestCounts.get(ip).filter(t => t > windowStart);
      
      if (times.length >= maxRequests) {
        return res.status(429).json({ error: 'Too many requests' });
      }
      
      times.push(now);
      requestCounts.set(ip, times);
      next();
    });

    app.get('/health', (req, res) => {
      const dbStatus = this.db?.getStatus?.() || { isDegraded: 'unknown' };
      const status = {
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        database: {
          connected: dbStatus.connectionType !== 'none',
          degraded: dbStatus.isDegraded,
          type: dbStatus.connectionType
        }
      };
      res.status(200).json(status);
    });

    app.get('/ready', (req, res) => {
      const dbStatus = this.db?.getStatus?.() || { isDegraded: true, connectionType: 'none' };
      const isReady = this.isReady() && !dbStatus.isDegraded;

      const metrics = {
        guildCount: this.guilds?.cache?.size ?? 0,
        commandCount: this.commands?.size ?? 0,
        database: {
          mode: dbStatus.connectionType,
          degraded: dbStatus.isDegraded,
          degradedReason: dbStatus.degradedReason ?? null,
        },
        schemaVersion: EXPECTED_SCHEMA_VERSION,
        schemaLabel: EXPECTED_SCHEMA_LABEL,
      };

      if (isReady) {
        return res.status(200).json({
          ready: true,
          message: 'Bot is ready',
          metrics,
        });
      }

      res.status(503).json({
        ready: false,
        reason: !this.isReady() ? 'Bot not Ready' : 'Database degraded',
        metrics,
      });
    });

    app.get('/', (req, res) => {
      res.status(200).json({ 
        message: 'TitanBot System Online',
        version: pkg.version,
        timestamp: new Date().toISOString()
      });
    });


    const dashboardAuth = (req, res, next) => {
      const expected = process.env.KREW_DASHBOARD_API_SECRET || '';
      const authorization = String(req.headers.authorization || '');
      const supplied = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
      let mismatch = expected.length ^ supplied.length;
      const length = Math.max(expected.length, supplied.length);
      for (let index = 0; index < length; index += 1) {
        mismatch |= (expected.charCodeAt(index) || 0) ^ (supplied.charCodeAt(index) || 0);
      }
      if (!expected) {
        return res.status(503).json({ error: 'Dashboard API secret is not configured.' });
      }
      if (mismatch !== 0) {
        return res.status(401).json({ error: 'Unauthorized.' });
      }
      next();
    };

    const isPlainObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
    const validText = (value, maxLength) => typeof value === 'string' && value.length <= maxLength;
    const validChannel = (guild, channelId) => {
      if (channelId === null) return true;
      if (typeof channelId !== 'string' || channelId.length > 32) return false;
      const channel = guild.channels.cache.get(channelId);
      return Boolean(channel && channel.isTextBased() && !channel.isThread());
    };
    const validEmbed = (embed) => {
      if (!isPlainObject(embed)) return false;
      if ('title' in embed && !validText(embed.title, 256)) return false;
      if ('footer' in embed && !validText(embed.footer, 2048)) return false;
      if ('color' in embed && (typeof embed.color !== 'string' || !/^#[0-9a-fA-F]{6}$/.test(embed.color))) return false;
      return true;
    };
    const serializeWelcomeConfig = (config) => ({
      enabled: Boolean(config.enabled),
      channelId: config.channelId ?? null,
      welcomeMessage: config.welcomeMessage ?? 'Welcome {user} to {server}!',
      welcomePing: Boolean(config.welcomePing),
      welcomeImage: config.welcomeImage ?? null,
      welcomeEmbed: {
        title: config.welcomeEmbed?.title ?? '🎉 Welcome!',
        footer: config.welcomeEmbed?.footer ?? 'Welcome to {server}!',
        color: config.welcomeEmbed?.color ?? '#7665ef',
      },
      goodbyeEnabled: Boolean(config.goodbyeEnabled),
      goodbyeChannelId: config.goodbyeChannelId ?? null,
      leaveMessage: config.leaveMessage ?? '{user.tag} has left the server.',
      goodbyePing: Boolean(config.goodbyePing),
      goodbyeEmbed: {
        title: config.leaveEmbed?.title ?? '👋 Goodbye',
        footer: config.leaveEmbed?.footer ?? 'Goodbye from {server}!',
        color: config.leaveEmbed?.color ?? '#ef6b83',
      },
      roleIds: Array.isArray(config.roleIds) ? config.roleIds : [],
      autoRoleDelay: Number(config.autoRoleDelay) || 0,
    });

    app.get('/dashboard/guilds', dashboardAuth, (req, res) => {
      const guilds = [...this.guilds.cache.values()].map((guild) => ({
        id: guild.id,
        name: guild.name,
        icon: guild.icon,
        memberCount: guild.memberCount || 0,
      }));
      return res.status(200).json({ guilds });
    });

    app.get('/dashboard/guilds/:guildId', dashboardAuth, async (req, res) => {
      try {
        const guild = this.guilds.cache.get(req.params.guildId);
        if (!guild) return res.status(404).json({ error: 'Krew is not in this server.' });

        const config = await getWelcomeConfig(this, guild.id);
        const channels = [...guild.channels.cache.values()]
          .filter((channel) => channel.isTextBased() && !channel.isThread() && [0, 5].includes(channel.type))
          .map((channel) => ({ id: channel.id, name: channel.name, type: channel.type }));
        const roles = [...guild.roles.cache.values()]
          .filter((role) => !role.managed && role.id !== guild.id)
          .sort((a, b) => b.position - a.position)
          .map((role) => ({
            id: role.id,
            name: role.name,
            color: role.color,
            position: role.position,
            managed: role.managed,
          }));

        return res.status(200).json({
          guild: {
            id: guild.id,
            name: guild.name,
            icon: guild.icon,
            memberCount: guild.memberCount || 0,
          },
          channels,
          roles,
          welcomeConfig: serializeWelcomeConfig(config),
        });
      } catch (error) {
        logger.error('Dashboard guild read failed:', error);
        return res.status(500).json({ error: 'Could not load this server.' });
      }
    });

    app.patch('/dashboard/guilds/:guildId/welcome', dashboardAuth, async (req, res) => {
      try {
        const guild = this.guilds.cache.get(req.params.guildId);
        if (!guild) return res.status(404).json({ error: 'Krew is not in this server.' });
        const body = req.body;
        if (!isPlainObject(body)) return res.status(400).json({ error: 'Invalid settings payload.' });

        const updates = {};
        if ('enabled' in body) {
          if (typeof body.enabled !== 'boolean') return res.status(400).json({ error: 'Invalid welcome toggle.' });
          updates.enabled = body.enabled;
        }
        if ('channelId' in body) {
          if (!validChannel(guild, body.channelId)) return res.status(400).json({ error: 'Choose a valid text channel.' });
          updates.channelId = body.channelId;
        }
        if ('welcomeMessage' in body) {
          if (!validText(body.welcomeMessage, 2000)) return res.status(400).json({ error: 'Welcome message must be 2,000 characters or fewer.' });
          updates.welcomeMessage = body.welcomeMessage;
        }
        if ('welcomePing' in body) {
          if (typeof body.welcomePing !== 'boolean') return res.status(400).json({ error: 'Invalid mention toggle.' });
          updates.welcomePing = body.welcomePing;
        }
        if ('welcomeImage' in body) {
          if (body.welcomeImage !== null) {
            if (!validText(body.welcomeImage, 500)) return res.status(400).json({ error: 'Image URL is too long.' });
            try {
              if (new URL(body.welcomeImage).protocol !== 'https:') throw new Error('https required');
            } catch {
              return res.status(400).json({ error: 'Enter a valid HTTPS image URL.' });
            }
          }
          updates.welcomeImage = body.welcomeImage;
        }
        if ('welcomeEmbed' in body) {
          if (!validEmbed(body.welcomeEmbed)) return res.status(400).json({ error: 'Invalid welcome embed settings.' });
          const current = await getWelcomeConfig(this, guild.id);
          updates.welcomeEmbed = { ...current.welcomeEmbed, ...body.welcomeEmbed };
        }

        const updated = await updateWelcomeConfig(this, guild.id, updates);
        return res.status(200).json({ ok: true, welcomeConfig: serializeWelcomeConfig(updated) });
      } catch (error) {
        logger.error('Dashboard welcome update failed:', error);
        return res.status(500).json({ error: 'Could not save welcome settings.' });
      }
    });

    app.patch('/dashboard/guilds/:guildId/goodbye', dashboardAuth, async (req, res) => {
      try {
        const guild = this.guilds.cache.get(req.params.guildId);
        if (!guild) return res.status(404).json({ error: 'Krew is not in this server.' });
        const body = req.body;
        if (!isPlainObject(body)) return res.status(400).json({ error: 'Invalid settings payload.' });

        const updates = {};
        if ('goodbyeEnabled' in body) {
          if (typeof body.goodbyeEnabled !== 'boolean') return res.status(400).json({ error: 'Invalid goodbye toggle.' });
          updates.goodbyeEnabled = body.goodbyeEnabled;
        }
        if ('goodbyeChannelId' in body) {
          if (!validChannel(guild, body.goodbyeChannelId)) return res.status(400).json({ error: 'Choose a valid text channel.' });
          updates.goodbyeChannelId = body.goodbyeChannelId;
        }
        if ('leaveMessage' in body) {
          if (!validText(body.leaveMessage, 2000)) return res.status(400).json({ error: 'Goodbye message must be 2,000 characters or fewer.' });
          updates.leaveMessage = body.leaveMessage;
        }
        if ('goodbyePing' in body) {
          if (typeof body.goodbyePing !== 'boolean') return res.status(400).json({ error: 'Invalid mention toggle.' });
          updates.goodbyePing = body.goodbyePing;
        }
        if ('leaveEmbed' in body) {
          if (!validEmbed(body.leaveEmbed)) return res.status(400).json({ error: 'Invalid goodbye embed settings.' });
          const current = await getWelcomeConfig(this, guild.id);
          updates.leaveEmbed = { ...current.leaveEmbed, ...body.leaveEmbed };
        }

        const updated = await updateWelcomeConfig(this, guild.id, updates);
        return res.status(200).json({ ok: true, welcomeConfig: serializeWelcomeConfig(updated) });
      } catch (error) {
        logger.error('Dashboard goodbye update failed:', error);
        return res.status(500).json({ error: 'Could not save goodbye settings.' });
      }
    });

    app.patch('/dashboard/guilds/:guildId/autoroles', dashboardAuth, async (req, res) => {
      try {
        const guild = this.guilds.cache.get(req.params.guildId);
        if (!guild) return res.status(404).json({ error: 'Krew is not in this server.' });
        const body = req.body;
        if (!isPlainObject(body) || !Array.isArray(body.roleIds) || body.roleIds.length > 10) {
          return res.status(400).json({ error: 'Choose up to 10 valid roles.' });
        }
        if (body.roleIds.some((id) => typeof id !== 'string' || !guild.roles.cache.has(id))) {
          return res.status(400).json({ error: 'One or more selected roles are not in this server.' });
        }
        const botMember = guild.members.me;
        const botHighest = botMember?.roles.highest?.position ?? 0;
        const selectedRoles = body.roleIds.map((id) => guild.roles.cache.get(id));
        if (selectedRoles.some((role) => !role || role.managed || role.id === guild.id || role.position >= botHighest)) {
          return res.status(400).json({ error: 'Krew cannot assign one or more selected roles. Move Krew’s role above them and try again.' });
        }
        if ('autoRoleDelay' in body && (!Number.isInteger(body.autoRoleDelay) || body.autoRoleDelay < 0 || body.autoRoleDelay > 3600)) {
          return res.status(400).json({ error: 'Role delay must be between 0 and 3,600 seconds.' });
        }

        const updates = { roleIds: body.roleIds };
        if ('autoRoleDelay' in body) updates.autoRoleDelay = body.autoRoleDelay;
        const updated = await updateWelcomeConfig(this, guild.id, updates);
        return res.status(200).json({ ok: true, welcomeConfig: serializeWelcomeConfig(updated) });
      } catch (error) {
        logger.error('Dashboard auto-role update failed:', error);
        return res.status(500).json({ error: 'Could not save auto-role settings.' });
      }
    });

    const startServer = (port, attempt = 0) => {
      let hasStartedListening = false;
      const server = app.listen(port, host, () => {
        hasStartedListening = true;
        this.webServer = server;
        startupLog(`✅ Web Server running on ${host}:${port}`);
        startupLog(`Health endpoint: http://${host}:${port}/health`);
        startupLog(`Ready endpoint: http://${host}:${port}/ready`);
      });

      server.on('error', (error) => {
        const errorCode = error?.code || 'UNKNOWN_ERROR';
        const errorMessage = error?.message || 'Unknown server error';

        if (!hasStartedListening && errorCode === 'EADDRINUSE' && attempt < maxPortRetryAttempts) {
          const nextPort = port + 1;
          startupLog(`Port ${port} is already in use. Trying port ${nextPort}...`);
          setTimeout(() => startServer(nextPort, attempt + 1), 250);
          return;
        }

        if (hasStartedListening && errorCode === 'EADDRINUSE') {
          logger.warn(`Web server reported a duplicate bind warning on ${host}:${port}, but the bot remains online.`);
          return;
        }

        logger.error(`❌ Web server error on port ${port} (${errorCode}): ${errorMessage}`);

        if (!hasStartedListening) {
          process.exit(1);
        }
      });
    };

    startServer(configuredPort, 0);
  }

  setupCronJobs() {
    cron.schedule('0 6 * * *', runSafeTask('birthday_check', () => checkBirthdays(this)));
    cron.schedule('* * * * *', runSafeTask('giveaway_check', () => checkGiveaways(this)));
    cron.schedule('*/15 * * * *', runSafeTask('counter_update', () => this.updateAllCounters()));
  }

  async updateAllCounters() {
    if (!this.db) {
      logger.warn('Database not available for counter updates');
      return;
    }
    
    for (const [guildId, guild] of this.guilds.cache) {
      try {
        const counters = await getServerCounters(this, guildId);
        const validCounters = [];
        const orphanedCounters = [];
        
        for (const counter of counters) {
          if (counter && counter.type && counter.channelId && counter.enabled !== false) {
            const channel = guild.channels.cache.get(counter.channelId);
            if (channel) {
              validCounters.push(counter);
              await updateCounter(this, guild, counter);
            } else {
              orphanedCounters.push(counter);
              logger.info(`Removing orphaned counter ${counter.id} (type: ${counter.type}, deleted channel: ${counter.channelId}) from guild ${guildId}`);
            }
          }
        }
        
        // Save cleaned counters if any were orphaned
        // Save cleaned counters if any were orphaned
        if (orphanedCounters.length > 0) {
          await saveServerCounters(this, guildId, validCounters);
          logger.info(`Cleaned up ${orphanedCounters.length} orphaned counter(s) from guild ${guildId} during scheduled update`);
        }
      } catch (error) {
        logger.error(`Error updating counters for guild ${guildId}:`, error);
      }
    }
  }

  async loadHandlers() {
    startupLog('Loading handlers...');
    const handlers = [
      { path: 'events', type: 'default', required: true },
      { path: 'interactions', type: 'default', required: true }
    ];

    for (const handler of handlers) {
      try {
        startupLog(`Loading handler: ${handler.path}`);
        const module = await import(`./handlers/loaders/${handler.path}.js`);
        const loaderFn = handler.type.startsWith('named:')
          ? module[handler.type.split(':')[1]]
          : module.default;

        if (typeof loaderFn === 'function') {
          await loaderFn(this);
          startupLog(`✅ Loaded ${handler.path}`);
        } else {
          throw new Error(`Invalid loader export from ${handler.path}`);
        }
      } catch (error) {
        if (handler.required) {
          logger.error(`❌ Failed to load required handler ${handler.path}:`, error.message);
          throw error;
        } else if (error.code !== 'MODULE_NOT_FOUND') {
          logger.warn(`⚠️  Failed to load optional handler ${handler.path}:`, error.message);
        }
      }
    }
  }

  async registerCommands() {
    try {
      await registerSlashCommands(this, { clientId: this.config.bot.clientId });
    } catch (error) {
      logger.error('Error registering commands:', error);
    }
  }

  async shutdown(reason = 'UNKNOWN') {
    shutdownLog(`Bot is shutting down (${reason})...`);
    logger.info(`\n${'='.repeat(60)}`);
    logger.info(`🛑 Graceful Shutdown Initiated (${reason})`);
    logger.info(`${'='.repeat(60)}`);

    try {
      
      logger.info('Stopping cron jobs...');
      cron.getTasks().forEach(task => task.stop());
      logger.info('✅ Cron jobs stopped');

      logger.info('Stopping music players...');
      await shutdownMusic(this);
      logger.info('✅ Music players stopped');

      if (this.webServer) {
        logger.info('Closing web server...');
        await new Promise((resolve) => this.webServer.close(resolve));
        logger.info('✅ Web server closed');
      }

      if (this.db && typeof this.db.disconnect === 'function') {
        logger.info('Closing database connection...');
        try {
          await this.db.disconnect();
          logger.info('✅ Database connection closed');
        } catch (error) {
          logger.warn('Error closing database connection:', error.message);
        }
      }

      logger.info('Destroying Discord client...');
      if (this.isReady()) {
        try {
          this.destroy();
          logger.info('✅ Discord client destroyed');
        } catch (error) {

          logger.warn('Discord client destroy warning (non-critical):', error.message);
        }
      }

      logger.info('✅ Graceful shutdown complete');
  shutdownLog('Bot stopped successfully.');
      process.exit(0);
    } catch (error) {
      logger.error('Error during graceful shutdown:', error);
      process.exit(1);
    }
  }
}

try {
  const bot = new TitanBot();
  
  const setupShutdown = () => {
    process.on('SIGTERM', () => bot.shutdown('SIGTERM'));
    process.on('SIGINT', () => bot.shutdown('SIGINT'));
    
    process.on('uncaughtException', (error) => {
      // Process state may be corrupt after an uncaught throw; log and shut down cleanly.
      handleTaskError('uncaught_exception', error, { fatal: true });
      bot.shutdown('UNCAUGHT_EXCEPTION');
    });

    process.on('unhandledRejection', (reason) => {
      const code = reason?.code;
      if (code === 10062 || code === 40060 || code === 50027) {
        logger.warn('Recoverable Discord interaction rejection:', reason?.message || reason);
        return;
      }
      if (reason?.message?.includes('Queue is empty')) {
        return;
      }

      // A stray rejection is a bug to fix, not a reason to take the bot down.
      // Log loudly with full context; the central task handler categorizes it.
      handleTaskError('unhandled_rejection', reason instanceof Error ? reason : new Error(String(reason)), {
        errorCode: ErrorCodes.UNHANDLED_REJECTION,
      });
    });
  };
  
  setupShutdown();
  bot.start().catch((error) => {
    logger.error('Fatal error during bot startup:', error);
    bot.shutdown('STARTUP_ERROR');
  });
} catch (error) {
  logger.error('Fatal error during bot startup:', error);
  process.exit(1);
}

export default TitanBot;